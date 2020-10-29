# Consumer

之前我们对消息队列体系中的注册中心、生产者、代理人(broker)有过初步的了解

+ broker启动后会将自己的信息注册进namesrv
+ producer启动后会通过namesrv获取broker信息
+ 消息生产后producer会将消息推送至broker
+ broker收到消息后, 执行刷盘策略, 构建consumerQueue和indexFile

以上, 我们的消息已经存在与broker中了, 接下来就是消费者如何去消费的问题

常见的消费方式就是推拉方式

推即broker收到消息后, 可能在执行刷盘策略前后将消息推送到consumer中, 假设rocketMQ使用的是推方式, 需要注意如下几个点:

+ broker如何准确的将消息推送到需要消费该消息的消费者手中
+ broker与consumer的连接如何建立 (consumer从namesrv得到broker信息后保持长连接? 我们已知namesrv是只注册broker的, 如果consumer也注册进namesrv, 那么broker是否可以获取consumer信息推送消息?)
+ broker如何判断consumer的消费能力

当然推方式是有推方式的好处的, 首先消息的即时性可以得到保证, 其次消费者的逻辑变得异常简单, 仅仅需要使用netty的read读取消息后走自己的业务逻辑处理消息

拉即broker收到消息后, 只需要做好消息的落盘操作, 在消费者有消费请求的时候, 再去根据请求中带有的参数定位消息然后返回, 但是拉方式有以下几点问题:

+ 对于即时性需求高的场景, 该方式有一定的延时性, 毕竟消费者无法得知生产者的生产时间
+ 消费者需要循环去拉取消息, 拉取时间间隔过长则导致消息堆积, 过短可能会导致消费能力不足

对于rocketMQ来说, 他采取了长轮询的拉方式来实现,

## broker保持拉取请求

在broker的启动流程中将pullRequestHoldService启动

```java
        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.start();
        }
```

该服务名称就叫保持拉取请求的服务

```java
        while (!this.isStopped()) {
            try {
                // broker允许长轮询就设置5秒钟的等待
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    this.waitForRunning(5 * 1000);
                } else {
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                // 检查保持的请求, 如果请求参数合法, 就去通知线程消息已经可以消费
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }
```

那么该长轮询是怎么做到的?

换句话说, 消费者调用rpc服务来获取消息的时候, broker中没有消息存在, 会阻塞吗?

我们找到broker的拉取消息处理器

```java
                case ResponseCode.PULL_NOT_FOUND:

                    if (brokerAllowSuspend && hasSuspendFlag) {
                        long pollingTimeMills = suspendTimeoutMillisLong;
                        if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                            pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
                        }

                        String topic = requestHeader.getTopic();
                        long offset = requestHeader.getQueueOffset();
                        int queueId = requestHeader.getQueueId();
                        // 这里创建了一个拉取请求对象放到holdService中
                        // 这里是有channel的, 如果消息到达broker之后会通过这个channel发送给消费者
                        PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills,
                            this.brokerController.getMessageStore().now(), offset, subscriptionData, messageFilter);
                        this.brokerController.getPullRequestHoldService().suspendPullRequest(topic, queueId, pullRequest);
                        response = null;
                        break;
                    }
```

在上述代码中, 我们可以获取以下信息:

+ broker处理消息消费都是先尝试获取消息
+ 如果尝试获取失败, 则根据状态码来做不同的处理
+ 如果状态码是PULL_NOT_FOUND, 则说明消费请求是正常的, 只是消息尚未产生
+ broker会创建一个pullRequest对象, 里面包含channel, 然后丢给PullRequestHoldService处理

这样一来, 消费端的线程就不会被长期阻塞, 这里设置response为null, 消费端可能会根据这个空值做处理, 后续我们再看



我们可以看见broker调用了suspendPullRequest, 将pullRequest传入了PullRequestHoldService中的pullRequestTable里面

接下去我们回到之前的checkHoldRequest方法中

```java
final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
```

首先获取了该队列的最大偏移量, 然后调用了notifyMessageArriving方法, 看名字我们可以发现是消息到达的处理

此时broker可能已经堆积了许多的pullRequest, 在该方法中可以对这些请求做批量处理

```java
// 偏移量合规则可以进行消息消费
                    if (newestOffset > request.getPullFromThisOffset()) {
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                            new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }
                        // 上述的规则都匹配则会将消息发送给消费者
                        if (match) {
                            try {
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }
                    }
```

对于每个pull请求, 在判断了offset是小于当前最大偏移量的时候, 则可以做消息请求与发送

在executeRequestWhenWakeup()方法中, 会重复调用一次处理器中的processRequest()方法, 就好比该请求是消费者重新发送的一样, 如果获取的response还是null, 则还是会重复上述流程, 等待消息到达

至此broker对于消息消费的处理已经结束, 总结就是以下几点:

- 消费者通过rpc将请求发送给broker后, 如果消息已经存在broker中, 则会直接通过这个请求将消息返回, 如果消息不存在则会生成一个pullRequest保存至broker中等待消息到达后使用
- 消费者无论消息是否存在broker中, 都不会阻塞
- broker会启动一个长轮询去轮询pullRequest队列, 如果消息到达则会通过request中的channel将消息推送给消费者

## consumer的消费发起

consumer通过mqFactory启动一个负载均衡服务, 在该服务中会不停的去调用doRebalance方法

在rebalance方法中, 会通过定时服务(consumer向broker上报心跳)中获取到当前客户端所分配到的消费队列

获取到消费队列之后, 会遍历这些消费队列封装成pullRequest放到阻塞队列中等待PullMessageService去获取

所以我们常说pushConsumer其实底层实现是pull但是用户并不需要去写pull的逻辑, 这是因为rocketMQ将这些封装了起来使得pushConsumer看上去像是broker中将消息推送过来的一样

而我们通过阅读broker消息消费的源码得知, broker如果没有消费者发起的消费请求是不会主动推送消息到消费者手中的

在rebalance中会获取topic下的mqSet即我们之前说的消费队列

```java
        for (MessageQueue mq : mqSet) {
            if (!this.processQueueTable.containsKey(mq)) {
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    continue;
                }

                this.removeDirtyOffset(mq);
                ProcessQueue pq = new ProcessQueue();
                // 这里会更新该mq的topic下的信息, topicSubscribeInfoTable
                long nextOffset = this.computePullFromWhere(mq);
                if (nextOffset >= 0) {
                    // 取出原先存在的value
                    // 如果value存在则加入消息请求队列
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        // 这里封装pullRequest
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        // 添加到请求队列中
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }
        }
this.dispatchPullRequest(pullRequestList);
```

通过心跳获取该消费端可以消费的消息队列后会封装成一个请求对象, 然后分派这些请求对象

最后这些请求会来到PullMessageService的pullRequestQueue阻塞队列中

这个拉取消息的服务在功mqClientFactory启动的时候就会被启动

我们来看看run方法

```java
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                // 循环去获取阻塞队列中的请求
                PullRequest pullRequest = this.pullRequestQueue.take();
                this.pullMessage(pullRequest);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                log.error("Pull Message Service Run Method exception", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }
```

我们可以看见, 消费者由rebalance生产拉取请求, 然后通过阻塞队列在这里被轮询获取

获取到拉取请求后, 调用pullMessage方法真正的发起请求去broker获取消息

接下去就是rpc的事情了, 可以同步或者异步的获取请求

broker端返回了消息之后

如果消息存在则会调用consumeMessageService的submitConsumeRequest方法

具体到不同的顺序消费或者并发消费, 我们看看并发消费的逻辑

```java
// 消息消费任务
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }
```

这里会创建一个任务, 然后提交给线程池进行, 我们看看这个任务的run方法

首先, 我们在pushConsumer创建的时候会注册一个监听器, 该监听器就是于consumer拉取到消息的时候被调用的

```java
// 获取创建消费者时创建的监听器
            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
// ...
            try {
                // 这里调用监听器的业务逻辑
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                hasException = true;
            }
```

在上述代码中, 我们可以看见我们设置的监听器在consumer获取到具体消息之后, 会调用 consumeMessage方法并且防区获取到的消息

这里的msg是以数组形式返回的, 通过元素中的getBody可以获取到消息的主体内容

```java
    @Test
    public void testPullMessage_Success() throws InterruptedException, RemotingException, MQBrokerException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final MessageExt[] messageExts = new MessageExt[1];
        // 注册监听器
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeMessageService(new ConsumeMessageConcurrentlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                messageExts[0] = msgs.get(0);
                countDownLatch.countDown();
                return null;
            }
        }));

        PullMessageService pullMessageService = mQClientFactory.getPullMessageService();
        pullMessageService.executePullRequestImmediately(createPullRequest());
        countDownLatch.await();
        assertThat(messageExts[0].getTopic()).isEqualTo(topic);
        assertThat(messageExts[0].getBody()).isEqualTo(new byte[] {'a'});
    }
```

以上是一段在rocketMQ的consumer的test中写的测试代码, 我们可以以此验证我们的想法

# 后记

rocketMQ的消费流程位于broker和consumer中

broker是处理consumer发起的请求, 将consumer传来的偏移量去commitLog中寻找消息, 如果没有找到消息则会保存这次请求并且先返回一个未找到消息的响应给consumer避免consumer等待

broker内部会有一个轮询, 轮询保存的请求, 如果消息存在了则会通过请求中的channel将消息刷给consumer

consumer需要从broker中获取自己所能消费的consumerQueue, consumerQueue中保存了消息的偏移量和消息的大小, consumer将这些信息传给broker即可等待消息的到达

本文只讲了pushConsumer的消费流程, 但是pullConsumer与pushConsumer的流程是一样的, 因为他们其实底层都是pull实现的