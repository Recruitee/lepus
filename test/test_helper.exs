ExUnit.start(assert_receive_timeout: 2000)

Mox.defmock(Lepus.Rabbit.TestClient, for: Lepus.Rabbit.Client)
Mox.defmock(Lepus.TestProducer, for: GenStage)
Mox.defmock(Lepus.TestConsumer, for: Lepus.Consumer)
Mox.defmock(Lepus.TestClient, for: Lepus.Client)
