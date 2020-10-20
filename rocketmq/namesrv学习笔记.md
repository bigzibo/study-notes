# nameServer

nameServer是一个服务中心, 用于broker的注册, 然后consumer和producer通过连接namesrv获取broker的信息, namesrv是无状态的节点, 这意味着它不会有主从之分

以下是官方文档对namesrv的概念说明

> 名称服务充当路由消息的提供者。生产者或消费者能够通过名字服务查找各主题相应的Broker IP列表。多个Namesrv实例组成集群，但相互独立，没有信息交换。

namesrv的启动需要设置启动参数

-c /home/rocketmq/conf/namesrv.properties 设置配置文件

在配置文件中可以设置rocketmqHome, 它的作用会在后面namesrv源码中有所体现

```java
    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {

        try {
            // 创建namesrv的控制器
            NamesrvController controller = createNamesrvController(args);
            // 启动控制器, 启动namesrv服务
            start(controller);
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }
```

main方法很简单, 他仅仅创建了一个nameSrv的控制器, 然后启动控制器而已

这里只给出重要的部分

```java
public static NamesrvController createNamesrvController(String[] args) throws IOException, JoranException {
        // 构建命令行选项nameServer地址和help
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        // 如果options中是help则打印help信息然后退出
        // 给options中添加c:{配置文件}和p:{打印配置信息}参数
        commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }

        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
    	// 设置了namesrv的端口信息
        nettyServerConfig.setListenPort(9876);
        if (commandLine.hasOption('c')) {
            // 获取配置文件地址
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                // 创建输入流读取配置文件
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                properties.load(in);
                // 通过反射设置对象参数
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);

                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);
        // rocketmqHome的值是从args中获取的, 启动时需要配置
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

        // remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }
```

controller的创建就是

1. 设置rocketmq的版本
2. 设置namesrv端口
3. 读取预设的配置文件

接下去就是controller的启动

```java
    public static NamesrvController start(final NamesrvController controller) throws Exception {

        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }
		// 做controller的初始化
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }
        // 添加结束的钩子函数
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                controller.shutdown();
                return null;
            }
        }));
		//启动
        controller.start();

        return controller;
    }
```

在controller的初始化中做了以下几件事情

1. 将namesrv的配置文件中设置的kvConfig读取出来
2. 开始初始化netty, 根据操作系统(linux是epoll)设置了bossLoop和selector
3. 根据配置中设置的线程数量设置并初始化了netty使用到的线程池
4. 注册了处理器(处理器用于netty的业务逻辑处理), 由于rocketmq的所有模块使用的netty程序都是相同的一段, 所以这里使用到了享元模式将netty中处理业务逻辑的部分抽离出来
5. 启动一个服务, 每十秒扫描一次停止活动的broker, 并且移除
6. 启动一个服务, 每十秒打印一次所有配置

以下是部分源码

```java
public boolean initialize() {

        this.kvConfigManager.load();

        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);

        this.remotingExecutor =
            Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));

        // 注册处理器, 这里注册了存放了注册broker的处理器
        this.registerProcessor();

        // 扫描停止活动的broker 10秒执行一次
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                NamesrvController.this.routeInfoManager.scanNotActiveBroker();
            }
        }, 5, 10, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                NamesrvController.this.kvConfigManager.printAllPeriodically();
            }
        }, 1, 10, TimeUnit.MINUTES);

        return true;
    }
```

在controller的启动流程中, 仅仅是做了两件事情

1. 启动netty服务
2. 如果文件监听服务存在则启动

namesrv提供的是服务, 所以只需要启动netty的服务端

```java
        ServerBootstrap childHandler =
            this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize())
                .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize())
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                            .addLast(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, handshakeHandler)
                            .addLast(defaultEventExecutorGroup,
                                encoder,
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                connectionManageHandler,
                                serverHandler
                            );
                    }
                });
```

可以看见这里设置了serverHandler这个处理器来处理事件

而serverHandler中的channelRead0方法调用了抽象类NettyRemotingAbstract的processMessageReceived方法

```java
public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            switch (cmd.getType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }
```

由于该方法被client和server都会调用, 所以需要判断消息是请求还是响应类型, 显然发送给server的消息都是请求类型

processRequestCommand该方法会判断消息中的code来判断使用何种处理器, 这里的处理器就是在初始化的时候注册的

```java
public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        // 根据RequestCode获取具体的处理器
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        // 没有获取到处理器就用预先设置的默认处理器
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessor : matched;
        final int opaque = cmd.getOpaque();

        if (pair != null) {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        doBeforeRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);
                        final RemotingResponseCallback callback = new RemotingResponseCallback() {
                            @Override
                            public void callback(RemotingCommand response) {
                                doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd, response);
                                if (!cmd.isOnewayRPC()) {
                                    if (response != null) {
                                        response.setOpaque(opaque);
                                        response.markResponseType();
                                        try {
                                            ctx.writeAndFlush(response);
                                        } catch (Throwable e) {
                                            log.error("process request over, but response failed", e);
                                            log.error(cmd.toString());
                                            log.error(response.toString());
                                        }
                                    } else {
                                    }
                                }
                            }
                        };
                        // 根据不同的处理器使用不同方法, 异步请求则异步处理
                        if (pair.getObject1() instanceof AsyncNettyRequestProcessor) {
                            AsyncNettyRequestProcessor processor = (AsyncNettyRequestProcessor)pair.getObject1();
                            processor.asyncProcessRequest(ctx, cmd, callback);
                        } else {
                            NettyRequestProcessor processor = pair.getObject1();
                            RemotingCommand response = processor.processRequest(ctx, cmd);
                            callback.callback(response);
                        }
                    } catch (Throwable e) {
                        log.error("process request exception", e);
                        log.error(cmd.toString());

                        if (!cmd.isOnewayRPC()) {
                            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                                RemotingHelper.exceptionSimpleDesc(e));
                            response.setOpaque(opaque);
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };
    }
```

## broker的注册

默认请求处理器中的switch存在如下代码

```java
case RequestCode.REGISTER_BROKER: // 注册broker
                Version brokerVersion = MQVersion.value2Version(request.getVersion());
                if (brokerVersion.ordinal() >= MQVersion.Version.V3_0_11.ordinal()) {
                    return this.registerBrokerWithFilterServer(ctx, request);
                } else {
                    return this.registerBroker(ctx, request);
                }
```

broker注册的时候, code会被设置为REGISTER_BROKER

namesrv识别到这一code就会进入注册方法, 在3.0.11版本后加入了请求过滤

> RocketMQ的消费者可以根据Tag进行消息过滤，也支持自定义属性过滤。消息过滤目前是在Broker端实现的，优点是减少了对于Consumer无用消息的网络传输，缺点是增加了Broker的负担、而且实现相对复杂。

消息过滤的实现本文暂时不细究

在registerBrokerWithFilterServer方法中设置了响应头, 调用了broker的注册方法

响应头信息尤为重要, 该响应头中带有高可用服务地址和master的地址, 该信息对于broker的高可用服务的通信有影响

下面看看registerBroker方法是如何注册的

```java
    public RegisterBrokerResult registerBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final Channel channel) {
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                // 获取该broker集群下面的所有broker名称
                Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
                if (null == brokerNames) {
                    brokerNames = new HashSet<String>();
                    this.clusterAddrTable.put(clusterName, brokerNames);
                }
                // 添加当前注册的broker
                brokerNames.add(brokerName);

                boolean registerFirst = false;

                // 此前如果有注册过就直接获取, 没注册过就注册
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null == brokerData) {
                    registerFirst = true;
                    brokerData = new BrokerData(clusterName, brokerName, new HashMap<Long, String>());
                    this.brokerAddrTable.put(brokerName, brokerData);
                }
                Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();
                //Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
                //The same IP:PORT must only have one record in brokerAddrTable
                // 主从broker的brokername是一样的
                // 这里用于从服务升级为主服务
                Iterator<Entry<Long, String>> it = brokerAddrsMap.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<Long, String> item = it.next();
                    // 如果原先该地址已经注册过, 并且brokerId与此前不同则从map中移除该元素
                    if (null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey()) {
                        it.remove();
                    }
                }
                
                String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
                registerFirst = registerFirst || (null == oldAddr);

                if (null != topicConfigWrapper
                    && MixAll.MASTER_ID == brokerId) {
                    if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.getDataVersion())
                        || registerFirst) {
                        ConcurrentMap<String, TopicConfig> tcTable =
                            topicConfigWrapper.getTopicConfigTable();
                        if (tcTable != null) {
                            for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                                // topicQueueTable中存入broker信息
                                this.createAndUpdateQueueData(brokerName, entry.getValue());
                            }
                        }
                    }
                }
			   // 设置broker的实时信息
                BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr,
                    new BrokerLiveInfo(
                        System.currentTimeMillis(),
                        topicConfigWrapper.getDataVersion(),
                        channel,
                        haServerAddr));
                if (null == prevBrokerLiveInfo) {
                    log.info("new broker registered, {} HAServer: {}", brokerAddr, haServerAddr);
                }

                if (filterServerList != null) {
                    if (filterServerList.isEmpty()) {
                        this.filterServerTable.remove(brokerAddr);
                    } else {
                        this.filterServerTable.put(brokerAddr, filterServerList);
                    }
                }

                // 如果是slave注册, 则会设置一个master地址
                if (MixAll.MASTER_ID != brokerId) {
                    String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (masterAddr != null) {
                        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(masterAddr);
                        if (brokerLiveInfo != null) {
                            result.setHaServerAddr(brokerLiveInfo.getHaServerAddr());
                            result.setMasterAddr(masterAddr);
                        }
                    }
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        }

        return result;
    }

```

注册broker的时候在namesrv中用0, 1 来标识了broker的主从, 并且会给从服务设置主服务的地址

## consumer根据topic获取broker信息

consumer获取broker信息的时候code会设置为GET_ROUTEINFO_BY_TOPIC

然后去namesrv获取该topic下的broker信息

```java
    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetRouteInfoRequestHeader requestHeader =
            (GetRouteInfoRequestHeader) request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        // 这里根据topic从topicQueueTable获取brokerName
        // 而后根据brokerName从brokerAddrTable获取broker信息
        TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(requestHeader.getTopic());

        if (topicRouteData != null) {
            if (this.namesrvController.getNamesrvConfig().isOrderMessageEnable()) {
                String orderTopicConf =
                    this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG,
                        requestHeader.getTopic());
                topicRouteData.setOrderTopicConf(orderTopicConf);
            }

            byte[] content = topicRouteData.encode();
            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic()
            + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return response;
    }
```

获取broker信息的过程很简单, 仅仅就是根据topic去获取之前注册broker的时候存入的信息就行了

个人感觉namesrv是整个rocketmq中最简单的模块, 毕竟之前是使用zookeeper做注册中心的, namesrv主要的功能就是接受broker的注册, 然后处理consumer和producer的获取broker信息请求