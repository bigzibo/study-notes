# broker

本文仅仅探讨broker的启动和高可用注册, 以及如何提供消息服务

这里有一个不理解的地方就是, broker的高可用服务并没有使用到netty, 而是使用了NIO的socket

broker就是个代理商, 生产者的消息发送给broker后, 消费者再去broker上面拉消息

rocketMQ的broker有高可用机制, 可以主从复制然后slave broker可以提供消息读取服务

主从broker的区别在于注册进namesrv的时候master的brokerId是0而slave的brokerId是1

namesrv会判断broker的brokerId来判断是否设置masterAddr

slave会根据这个masterAddr与master建立一个socket连接, 根据偏移量来拉取消息备份到自己的服务器上

## controller

broker与namesrv一样也是通过controller来启动服务的

在broker的启动类中main方法先创建了controller然后再调用start方法启动controller

下面只提取一些重要部分代码

```java
    public static BrokerController createBrokerController(String[] args) {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        try {
            //PackageConflictDetect.detectFastjson();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options),
                new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
            }

            final BrokerConfig brokerConfig = new BrokerConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();

            nettyClientConfig.setUseTLS(Boolean.parseBoolean(System.getProperty(TLS_ENABLE,
                String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING))));
            nettyServerConfig.setListenPort(10911);
            // messageStore用于消息的存放与处理
            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

            if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }

            // 读取配置中设置的配置文件地址, 并且加载成javaBean
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);

                    properties2SystemEnv(properties);
                    MixAll.properties2Object(properties, brokerConfig);
                    MixAll.properties2Object(properties, nettyServerConfig);
                    MixAll.properties2Object(properties, nettyClientConfig);
                    MixAll.properties2Object(properties, messageStoreConfig);

                    BrokerPathConfigHelper.setBrokerConfigPath(file);
                    in.close();
                }
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);
            // rocketMQHome是必须设置的配置
            if (null == brokerConfig.getRocketmqHome()) {
                System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
                System.exit(-2);
            }
            // namesrv地址也是配置在配置文件中的
            String namesrvAddr = brokerConfig.getNamesrvAddr();
            if (null != namesrvAddr) {
                try {
                    // 获取名称服务地址, 配置中多地址用分号;分割
                    String[] addrArray = namesrvAddr.split(";");
                    for (String addr : addrArray) {
                        RemotingUtil.string2SocketAddress(addr);
                    }
                } catch (Exception e) {
                    System.out.printf(
                        "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                        namesrvAddr);
                    System.exit(-3);
                }
            }

            switch (messageStoreConfig.getBrokerRole()) {
                // 主服务都要设置brokerId
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                // 从服务的brokerId不能小于等于0
                // 所以slave的brokerId可以是1以上的任何数字
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= 0) {
                        System.out.printf("Slave's brokerId must be > 0");
                        System.exit(-3);
                    }

                    break;
                default:
                    break;
            }

            if (messageStoreConfig.isEnableDLegerCommitLog()) {
                brokerConfig.setBrokerId(-1);
            }
            // 设置高可用的端口, 默认是netty服务端端口+1
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
		   // 省略日志

            // 根据设置的config创建broker
            final BrokerController controller = new BrokerController(
                brokerConfig,
                nettyServerConfig,
                nettyClientConfig,
                messageStoreConfig);
            // remember all configs to prevent discard
            controller.getConfiguration().registerConfig(properties);
            // 对controller进行初始化
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);

                @Override
                public void run() {
                    synchronized (this) {
                        log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long beginTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                            log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }
```

其中最重要的部分就是controller的初始化

初始化中创建了netty相关的服务, 初始化了rocketMQ支持的事务消息和消息确认, 并且注册了rpc的钩子函数, 注册了netty事件的处理器

加载了消息仓库, messageStore中启动了commitLog和messageQueue

```java
public boolean initialize() throws CloneNotSupportedException {
        boolean result = this.topicConfigManager.load();

        result = result && this.consumerOffsetManager.load();
        result = result && this.subscriptionGroupManager.load();
        result = result && this.consumerFilterManager.load();

        if (result) {
            try {
                // 创建消息存储仓库
                this.messageStore =
                    new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener,
                        this.brokerConfig);
                if (messageStoreConfig.isEnableDLegerCommitLog()) {
                    DLedgerRoleChangeHandler roleChangeHandler = new DLedgerRoleChangeHandler(this, (DefaultMessageStore) messageStore);
                    ((DLedgerCommitLog)((DefaultMessageStore) messageStore).getCommitLog()).getdLedgerServer().getdLedgerLeaderElector().addRoleChangeHandler(roleChangeHandler);
                }
                this.brokerStats = new BrokerStats((DefaultMessageStore) this.messageStore);
                //load plugin
                MessageStorePluginContext context = new MessageStorePluginContext(messageStoreConfig, brokerStatsManager, messageArrivingListener, brokerConfig);
                this.messageStore = MessageStoreFactory.build(context, this.messageStore);
                this.messageStore.getDispatcherList().addFirst(new CommitLogDispatcherCalcBitMap(this.brokerConfig, this.consumerFilterManager));
            } catch (IOException e) {
                result = false;
                log.error("Failed to initialize", e);
            }
        }
        // 加载消息存储仓库, 包括commitLog和messageQueue
        result = result && this.messageStore.load();

        if (result) {
            // 创建netty的server
            this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
            NettyServerConfig fastConfig = (NettyServerConfig) this.nettyServerConfig.clone();
            fastConfig.setListenPort(nettyServerConfig.getListenPort() - 2);
            // 创建快速服务, 可以理解为vip服务, 其配置除了端口以外与上面的都一样
            this.fastRemotingServer = new NettyRemotingServer(fastConfig, this.clientHousekeepingService);
            // 这里注释了开启的一系列线程池的初始化

            // 注册处理器, 十分重要的一步
            this.registerProcessor();

            final long initialDelay = UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis();
            final long period = 1000 * 60 * 60 * 24;
            // 一天运行一次的记录任务, 负责打印一天发送和接收了多少消息
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.getBrokerStats().record();
                    } catch (Throwable e) {
                        log.error("schedule record error.", e);
                    }
                }
            }, initialDelay, period, TimeUnit.MILLISECONDS);

            // 这里注释了一系列开启的定时服务
            
            // 往brokerOuterAPI中更新名称服务地址
            // 该类提供了broker与namesrv一系列的交互api
            if (this.brokerConfig.getNamesrvAddr() != null) {
                this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
                log.info("Set user specified name server address: {}", this.brokerConfig.getNamesrvAddr());
            } else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
                // 如果namesrv没有配置就定时去刷新namesrv地址
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                        } catch (Throwable e) {
                            log.error("ScheduledTask fetchNameServerAddr exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
            }

            if (!messageStoreConfig.isEnableDLegerCommitLog()) {
                // 从服务器得配置主从备份
                if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                    if (this.messageStoreConfig.getHaMasterAddress() != null && this.messageStoreConfig.getHaMasterAddress().length() >= 6) {
                        this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                        this.updateMasterHAServerAddrPeriodically = false;
                    } else {
                        // 如果一开始没有配置高可用地址, 则从namesrv中获取
                        this.updateMasterHAServerAddrPeriodically = true;
                    }
                } else {
                    this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                BrokerController.this.printMasterAndSlaveDiff();
                            } catch (Throwable e) {
                                log.error("schedule printMasterAndSlaveDiff error.", e);
                            }
                        }
                    }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
                }
            }
            // 初始化事务消息
            initialTransaction();
            // 初始化消息确认
            initialAcl();
            // 初始化注册rpc钩子函数
            initialRpcHooks();
        }
        return result;
    }
```

初始化controller之后就可以直接调用controller的start方法启动一系列服务

## start

start方法是开启controller在初始化过程中注册的一系列服务以及将自己注册进namesrv的过程

```java
    public void start() throws Exception {
        // 启动信息存储, 这里开启了主从复制
        if (this.messageStore != null) {
            this.messageStore.start();
        }

        if (this.remotingServer != null) {
            this.remotingServer.start();
        }

        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.start();
        }

        if (this.fileWatchService != null) {
            this.fileWatchService.start();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.start();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }

        if (this.filterServerManager != null) {
            this.filterServerManager.start();
        }

        // 不开启DLeger, 默认情况下使用commitLog和HaService
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            startProcessorByHa(messageStoreConfig.getBrokerRole());
            handleSlaveSynchronize(messageStoreConfig.getBrokerRole());
            this.registerBrokerAll(true, false, true);
        }

        // 将broker注册入namesrv, 重新注册时间为10~60秒
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    BrokerController.this.registerBrokerAll(true, false, brokerConfig.isForceRegister());
                } catch (Throwable e) {
                    log.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS);

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.start();
        }


    }
```

### message的start

message的start主要是启动了一系列关于服务器消息存储的功能例如设置commitLog的偏移量等等

后续了解mq的消息存储与读取的话, 这里可以作为入口

```java
        // 启动高可用服务 即主从复制, master和slave都会进入
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService.start();
            this.handleScheduleMessageService(messageStoreConfig.getBrokerRole());
        }
```

不过本文介绍的是启动和主从复制, 在messageStore中启动了高可用服务, 不论是master还是slave都会启动高可用服务

### HAService

```java
    public void start() throws Exception {
        this.acceptSocketService.beginAccept();
        // 建立socket连接
        this.acceptSocketService.start();
        // 同步双写
        this.groupTransferService.start();
        this.haClient.start();
    }
		// 做一系列socket连接的准备动作
        public void beginAccept() throws Exception {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            this.serverSocketChannel.configureBlocking(false);
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 一秒选择一次
                    this.selector.select(1000);
                    // 用来获取就绪的channel
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        // 遍历期间就绪的channel也会被遍历
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                // 获取socketChannel
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        // 高可用服务会通过这个channel进行读写操作
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        // 添加连接到队列中, 方便shutdown
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
```

可以发现, 高可用服务的socket连接并没有使用netty, 而是直接使用nio进行开发的

高可用读写的细节后续会专门写一篇文章来讲述

下面重点看一下HAClient这个类

```java
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");
            // 循环去master拉取数据直到服务被shutdown
            while (!this.isStopped()) {
                try {
                    // 如果是slave对master发起连接请求, 如果是master会不存在masterAddr, 这里永false
                    // 这里的masterAddr是namesrv中设置的
                    if (this.connectMaster()) {
                        // 判断是否需要发送心跳
                        if (this.isTimeToReportOffset()) {
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        this.selector.select(1000);
                        // 读事件处理
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }
                        // 判断是否需要报告偏移量, 报告方法报错才会为false
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }
                        // 当前时间到最后写入时间的差值
                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        // 默认是超过20秒没有读取就关闭连接, 这里在读事件时间超过20s并且不需要报告偏移量时才可能触发
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
```

下面是读事件的处理

读取也是一个循环的过程, 只要在这个过程中channel中存在数据, 就会一直读取

```java
private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    // 用nio的socket去读取数据
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        // 处理读取来的消息
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) { // 没有数据可读
                        // 如果读不到数据的次数大于3次, 则跳出循环
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }
```

主从复制的请求会带有请求头, 请求头的内容长度是8+4, 8是master的物理偏移量, 4是本次消息的长度

```java
private boolean dispatchReadRequest() {
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            int readSocketPos = this.byteBufferRead.position();
            // buffer中数据可能很多, 需要分次处理
            while (true) {
                // 获取增长
                int diff = this.byteBufferRead.position() - this.dispatchPosition;
                // 如果没有body就不处理
                if (diff >= msgHeaderSize) {
                    // 获取8位偏移量
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    // 获取4位长度
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);
				  // 获取slave的commitLog的偏移量
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        // 如果获取的master偏移量与现在slave中的偏移量不同
                        // 则会返回false并且打印错误日志
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }
                    // 这里理解为 diff >= this.byteBufferRead.position() - this.dispatchPosition
                    if (diff >= (msgHeaderSize + bodySize)) {
                        byte[] bodyData = new byte[bodySize];
                        // 设置指针到消息内容的起始位置
                        this.byteBufferRead.position(this.dispatchPosition + msgHeaderSize);
                        // buffer中数据写入bodyData
                        this.byteBufferRead.get(bodyData); 			
                       // 从slave的commitLog的masterPhyOffset位置开始写入bodyData
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);
                        // buffer重定位回初始位置(上面处理消息前获取的buffer的最后位置)
                        this.byteBufferRead.position(readSocketPos);
                        // 累加已处理的position
                        this.dispatchPosition += msgHeaderSize + bodySize;

                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }
                // 超出容量则重新申请空间
                // 这里会将位置指针清零
                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }
```

### master与slave的通信

这里探讨一下master如何获取slave偏移量的

在HAClient这个类的run方法中, 发送心跳即报告偏移量, 如果判断需要发送的时候会调用reportSlaveMaxOffset方法

```java
private boolean reportSlaveMaxOffset(final long maxOffset) {
    		// 偏移量是8位
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            // 发送三次偏移量, 应该是为了确保可以收到该偏移量
            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            return !this.reportOffset.hasRemaining();
        }
```

这里将slave的偏移量写入了与master建立的socket连接中

看看master如何接收处理

在HAService的start方法中, 我们可以看见启动了一个AcceptSocketService, 该类中将socket连接封装进了HAConnection中, 可以看见HAConnection中存在read和writer类

我们需要进入ReadSocketService中看一看, 经过以上这么多源码的阅读, 可以很快明白, 在run方法中会有一个循环体, 去处理读取事件

```java
while (!this.isStopped()) {
                    this.selector.select(1000);
                    // 读取事件
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }
                    //...
            }
```

processReadEvent方法进行了数据的读取与处理

```java
private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
			// buffer指针超出限制则做一下翻转读取
            if (!this.byteBufferRead.hasRemaining()) {
                this.byteBufferRead.flip();
                this.processPosition = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        // 查看是否起码有一个long数据, 因为传入的数据是slave上报的偏移量
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {
                            // 用于计算完整包位置, 应对沾包和半包
                            // 如果模除有余数说明是半包或者沾包
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            // slave上报的偏移量
                            // 这里读取的是去除沾包后的最后八位, 即一个偏移量
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            this.processPosition = pos;

                            // 同步偏移量
                            HAConnection.this.slaveAckOffset = readOffset;
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                // 初始为-1 说明还没获取, 现在获取了就改为上报的偏移量
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            // 唤醒相关消息发送线程
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
```

这里处理了沾包和半包的情况, 后续如果接收到数据就会拼接处理

下面看看master的写服务, slave读取的消息就是WriteSocketService这个服务类发送的

run方法中的消息处理部分

```java
while (!this.isStopped()) {
                try {
                    this.selector.select(1000);

                    // 刚初始化的时候还未获取偏移量, 等待
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }
                    // 如果是-1说明线程是第一次进行数据传输
                    if (-1 == this.nextTransferFromWhere) {
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            // 获取commitLog中最后读取到的偏移量
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            // 得到初始偏移量
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    // 判断最后一次写事件是否完成
                    if (this.lastWriteOver) {

                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        // 发送心跳, 心跳仅仅只有请求头
                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {

                            // Build Header
                            this.byteBufferHeader.position(0);
                            // 这里也是8+4对应了slave获取到的请求头长度
                            this.byteBufferHeader.limit(headerSize);
                            // 设置了master的偏移量长度是8
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            // 放入了一个int数据长度是4
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    // 获取需要同步的消息数据
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        this.nextTransferFromWhere += size;

                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        // 这里与上面一样
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        // 消息传送
                        this.lastWriteOver = this.transferData();
                    } else {

                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }
```

最后调用的transferData方法来发送消息

transferData的功能十分简单, 仅仅是做了一些长度判断然后就将run中处理好的header和body写入channel

```java
        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {
                // 把设置的请求头写入channel
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                // writeSize应该等于12
                if (writeSize > 0) {
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            if (null == this.selectMappedBufferResult) {
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    // run方法中获取了selectResult写入selectMappedBufferResult, 这里将selectMappedBufferResult写入channel
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }
```

## netty服务启动

这一部分主要是用于给MQClient提供的netty服务, 即生产者推送消息到broker和消费者从broker中拉取消息

在controller初始化的时候已经注册过处理器, 注册的处理器异常多, 最后使用哪个处理器是根据请求中的code判断的, 将code作为key, 具体处理器对象作为value存入processorTable这个map中

与namesrv的消息处理一样, 通过NettyServerHandler的read0方法, 调用processMessageReceived

在processMessageReceived方法中有具体的业务处理逻辑

```java
        // 根据RequestCode获取具体的处理器
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        // 没有获取到处理器就用预先设置的默认处理器
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessor : matched;
```

最后不同的消息类型用不同处理器处理

这一部分业务逻辑后续学习consumer和producer的时候会继续阅读

---

前文中提到的brokerOutAPI其实就是broker的nettyClient, 用于与namesrv通信

类似注册broker这一类的时间 都是通过这个类实现的

# 后记

broker中真正难的地方我认为其实应该是消息存储的实现, 这才是rocketMQ为什么快的地方
后续研究一下commitLog相关的内容