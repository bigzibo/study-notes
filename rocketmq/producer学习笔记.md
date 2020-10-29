# Producer

名词解释与设计架构可以阅读官方文档

[https://github.com/apache/rocketmq/tree/master/docs/cn]: https://github.com/apache/rocketmq/tree/master/docs/cn

生产者由于是用户使用的, 所以自然是用户创建的, 设置参数包括生产者的启动都应该是用户来进行,

所以我们从DefaultMQProducerTest默认的MQ生产者测试类作为入口看看producer的启动流程, 并且看看producer与broker的消息交互

## start()

在测试类中, 有一个初始化方法init(), 其中给producer设置了namesrv地址, 设置了消息压缩的阈值, 设置了消息的topic, 然后调用了producer的start方法

在start方法中仅仅设置了生产者集群名称而后调用了impl的start

在impl中有两个start方法, 一个是无参的, 无参的start方法启动时会启动MQClientFactory

而启动MQClientFactory的时候又会启动impl, 所以这是调用有参方法, 跳过启动工厂的步骤

```java
    public void start() throws MQClientException {
        this.start(true);
    }

public void start(final boolean startFactory) throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                
				// 省略部分内容
                
                // 获取工厂单例
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);

                // 将生产者注册进工厂
                // 这里会放到工厂对象中的producerTable里面
                boolean registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
                // 如果注册失败说明相同名称的生产者已经启动过
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    throw new MQClientException("The producer group[" + this.defaultMQProducer.getProducerGroup()
                        + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                        null);
                }

               // key为topic, value为发布信息存入map中
                this.topicPublishInfoTable.put(this.defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());

                // 启动mq客户端工厂
                if (startFactory) {
                    mQClientFactory.start();
                }

                log.info("the producer [{}] start OK. sendMessageWithVIPChannel={}", this.defaultMQProducer.getProducerGroup(),
                    this.defaultMQProducer.isSendMessageWithVIPChannel());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The producer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }

        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    RequestFutureTable.scanExpiredRequest();
                } catch (Throwable e) {
                    log.error("scan RequestFutureTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }
```

最后在clientFactory中会从namesrv获取broker信息, 启动生产消费服务, 启动消息交互的channel

```java
       /**
     * 不论是生产者还是消费者, 始终都是属于mq的客户端, 所以MQClient的start会启动拉取推送两个服务
     * 在实际生产中, 有很多生产者同时也会扮演消费者的角色去broker中拉取数据, 所以这里一次性启动
     */
    public void start() throws MQClientException {

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;
                    // If not specified,looking address from name server
                    if (null == this.clientConfig.getNamesrvAddr()) {
                        this.mQClientAPIImpl.fetchNameServerAddr();
                    }
                    // Start request-response channel
                    this.mQClientAPIImpl.start();
                    // Start various schedule tasks
                    this.startScheduledTask();
                    // Start pull service
                    this.pullMessageService.start(); // 生产者在take pullMessage的时候会被阻塞
                    // Start rebalance service
                    this.rebalanceService.start(); // 启动负载均衡服务, 消费者通过该服务选取消费队列进行消费
                    // Start push service
                    // 启动push服务, 这里启动的是group为CLIENT_INNER_PRODUCER的生产者
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                    log.info("the client factory [{}] start OK", this.clientId);
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

```

上文中是生产者消费者都需要经过的历程, 而生产者启动完毕后, 可以看见有start了一个生产者, 至于这个生产者有什么用, 后文中会做说明

接下来看看定时任务的启动

```java
private void startScheduledTask() {
    	// 基本都是设置了namesrvAddr的, 所以这里不需要去做获取操作
        if (null == this.clientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                    } catch (Exception e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

    	// 定期去namesrv中更新topic的broker信息
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.updateTopicRouteInfoFromNameServer();
                } catch (Exception e) {
                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);

    	// 清理离线的broker并且发送心跳给所有在线broker
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.cleanOfflineBroker();
                    MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
                } catch (Exception e) {
                    log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                }
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.adjustThreadPool();
                } catch (Exception e) {
                    log.error("ScheduledTask adjustThreadPool exception", e);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

```

生产者到此位置已经启动完毕

接下去看看发送消息的流程

## 消息生产

发送消息有同步和异步两种方式

同步发送需要阻塞线程, 而异步会使用future+监听器做异步处理, 线程可以直接离开无需阻塞

发送的调用链比较长, 我们撇去其他例如获取broker的操作, 只看发送



在这里第一次判断了发送模式, 有同步异步和单方

```java
switch (communicationMode) {
                    case ASYNC:
                        Message tmpMessage = msg;
                        boolean messageCloned = false;
                        if (msgBodyCompressed) {
                            //If msg body was compressed, msgbody should be reset using prevBody.
                            //Clone new message using commpressed message body and recover origin massage.
                            //Fix bug:https://github.com/apache/rocketmq-externals/issues/66
                            tmpMessage = MessageAccessor.cloneMessage(msg);
                            messageCloned = true;
                            msg.setBody(prevBody);
                        }

                        if (topicWithNamespace) {
                            if (!messageCloned) {
                                tmpMessage = MessageAccessor.cloneMessage(msg);
                                messageCloned = true;
                            }
                            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
                        }

                        long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeAsync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                            brokerAddr,
                            mq.getBrokerName(),
                            tmpMessage,
                            requestHeader,
                            timeout - costTimeAsync,
                            communicationMode,
                            sendCallback,
                            topicPublishInfo,
                            this.mQClientFactory,
                            this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed(),
                            context,
                            this);
                        break;
                    case ONEWAY:
                    case SYNC:
                        long costTimeSync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeSync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                            brokerAddr,
                            mq.getBrokerName(),
                            msg,
                            requestHeader,
                            timeout - costTimeSync,
                            communicationMode,
                            context,
                            this);
                        break;
                    default:
                        assert false;
                        break;
                }
```

继续进入sendMessage中



oneway和async的区别在于, async会设置监听器和futrue来处理返回结果, 而oneway直接舍弃返回结果, 是耗时最短的一种

```java
switch (communicationMode) {
            // 只管发送成功, 不管返回
            case ONEWAY:
                this.remotingClient.invokeOneway(addr, request, timeoutMillis);
                return null;
            // 线程设置future 等待返回后直接离开
            case ASYNC:
                final AtomicInteger times = new AtomicInteger();
                long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTimeAsync) {
                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
                }
                this.sendMessageAsync(addr, brokerName, msg, timeoutMillis - costTimeAsync, request, sendCallback, topicPublishInfo, instance,
                    retryTimesWhenSendFailed, times, context, producer);
                return null;
            // 线程同步等待返回
            case SYNC:
                long costTimeSync = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTimeSync) {
                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
                }
                return this.sendMessageSync(addr, brokerName, msg, timeoutMillis - costTimeSync, request);
            default:
                assert false;
                break;
        }
```

接下去都是很基础的消息通信部分, 发起io操作成功之后异步模式设置了监听器和future去等待结果后, 自己返回做其他任务

而同步模式则会自己等待结果, 阻塞线程

## broker接收消息

既然是通过网络传输的, 那么我们只需要找到broker是使用哪一个handler来处理接收消息的事件即可找到入口

我可以发现, producer发送消息给broker在header中的code是SEND_MESSAGE, 我们找到broker注册处理器的部分可以发现, SEND_MESSAGE的value是SendMessageProcessor类

```java
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = null;
        try {
            // response阻塞获取结果
            response = asyncProcessRequest(ctx, request).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("process SendMessage error, request : " + request.toString(), e);
        }
        return response;
    }

    public CompletableFuture<RemotingCommand> asyncProcessRequest(ChannelHandlerContext ctx,
                                                                  RemotingCommand request) throws RemotingCommandException {
        final SendMessageContext mqtraceContext;
        switch (request.getCode()) {
            case RequestCode.CONSUMER_SEND_MSG_BACK:
                return this.asyncConsumerSendMsgBack(ctx, request);
            // SEND_MESSAGE
            default:
                // 处理请求头
                SendMessageRequestHeader requestHeader = parseRequestHeader(request);
                if (requestHeader == null) {
                    return CompletableFuture.completedFuture(null);
                }
                mqtraceContext = buildMsgContext(ctx, requestHeader);
                this.executeSendMessageHookBefore(ctx, request, mqtraceContext);
                if (requestHeader.isBatch()) {
                    return this.asyncSendBatchMessage(ctx, request, mqtraceContext, requestHeader);
                } else {
                    // 单个消息进入该方法
                    return this.asyncSendMessage(ctx, request, mqtraceContext, requestHeader);
                }
        }
    }

	// 消息解析request中内容后封装成msgInner, 由此存入commitLog
	putMessageResult = this.brokerController.getMessageStore().asyncPutMessage(msgInner);

    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
		// 省略部分代码
        
        // 调用commitLog的asyncPutMessage方法
        CompletableFuture<PutMessageResult> putResultFuture = this.commitLog.asyncPutMessage(msg);

        putResultFuture.thenAccept((result) -> {
            long elapsedTime = this.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                log.warn("putMessage not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, msg.getBody().length);
            }
            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
            }
        });

        return putResultFuture;
    }
```

归根结底, broker获取消息后, 经过一系列的处理, 最终会将消息存入commitLog

asyncPutMessage方法很长, 我们关注一下写入部分

这里提一嘴, 在磁盘中顺序写的速度很快, 几乎是等于内存的写速度的, 所以rocketMQ保证了对commitLog的顺序写

```java
// 加锁保证顺序写
        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // 设置时间戳保证全局有序
            msg.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock = 0;
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
            }
            // appendMessage, 将消息加入队尾
            result = mappedFile.appendMessage(msg, this.appendMessageCallback);
            // 省略对返回结果做处理
            
            
                       elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }
```

# 后记

后续内容都是commitLog的处理了.. 暂时触及到了盲区,  后续会着重学习

但是我们已经可以发现了, 在rocketMQ中, 消息是被顺序写入commitLog的

后续会同时补充同步和异步刷盘的ack返回内容

