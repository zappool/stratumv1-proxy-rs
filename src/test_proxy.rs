use crate::client_stub::ClientStub;
use crate::server_stub::ServerStub;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn test_client_stub_connect_only() {
    let server_addr = "127.0.0.1:43333";
    let server = ServerStub::new(server_addr);
    let _ = server.start().await.unwrap();

    let mut client = ClientStub::new(server_addr, "username.device");
    let _ = client.start().await.unwrap();
    let _ = client.stop(false).await.unwrap();

    let _ = server.stop(true).await.unwrap();

    assert_eq!(server.get_connect_count().await, 1);
    assert_eq!(server.get_message_count().await, 0);
    assert_eq!(client.get_message_count().await, 0);
}

#[tokio::test]
#[serial]
async fn test_client_stub_mining_init() {
    let server_addr = "127.0.0.1:43333";
    let server = ServerStub::new(server_addr);
    let _ = server.start().await.unwrap();

    let mut client = ClientStub::new(server_addr, "username.device");
    let _ = client.start().await.unwrap();
    let _ = client.send_mining_configure().await.unwrap();
    let _ = client.send_mining_subscribe().await.unwrap();
    let _ = client.send_mining_authorize().await.unwrap();
    let _ = client.send_mining_suggest_difficulty(1000).await.unwrap();
    let _ = client.send_mining_submit().await.unwrap();
    let _ = client.stop(true).await.unwrap();

    let _ = server.stop(true).await.unwrap();

    // Now check what did the stub receive
    assert_eq!(server.get_connect_count().await, 1);
    assert_eq!(server.get_message_count().await, 5);
    let msg1 = server.get_message("1").await.unwrap();
    assert_eq!(msg1.method, "mining.configure");
    let msg2 = server.get_message("2").await.unwrap();
    assert_eq!(msg2.method, "mining.subscribe");
    let msg3 = server.get_message("3").await.unwrap();
    assert_eq!(msg3.method, "mining.authorize");
    let msg4 = server.get_message("4").await.unwrap();
    assert_eq!(msg4.method, "mining.suggest_difficulty");
    let msg5 = server.get_message("5").await.unwrap();
    assert_eq!(msg5.method, "mining.submit");

    assert_eq!(client.get_message_count().await, 4);
    let resp1 = client.get_message("1").await.unwrap();
    assert_eq!(
        resp1.to_string(),
        "1 null {\"version-rolling.mask\":\"1fffe000\"}"
    );
    let resp5 = client.get_message("5").await.unwrap();
    assert_eq!(resp5.to_string(), "5 null true");
}
