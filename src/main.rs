extern crate rusoto_core;
extern crate rusoto_sqs;

use std::default::Default;

use rusoto_core::Region;
use rusoto_sqs::{
    GetQueueAttributesRequest, GetQueueUrlRequest, ListQueuesRequest, ReceiveMessageRequest,
    SendMessageRequest, DeleteMessageRequest
};
use rusoto_sqs::{Sqs, SqsClient};
// use rusoto_sqs::{ CreateQueueRequest,
//     DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, SendMessageBatchRequest,
//     SendMessageBatchRequestEntry,
// };

static QUEUE_NAME: &str = "jdo-test-queue";

#[tokio::main]
async fn main() {
    let client: SqsClient = SqsClient::new(Region::UsEast1);

    list_queues(&client).await;
    let queue_url = get_queue_url(&client).await;
    get_queue_attributes(&client, &queue_url).await;
    //   send_message(&client, queue_url.clone(), "This is a test".to_string()).await;
    recv_message(&client, &queue_url).await;

    loop{}
}

async fn recv_message(client: &SqsClient, queue_url: &String) {
    let req = ReceiveMessageRequest {
        queue_url: queue_url.clone(),
        message_attribute_names: Some(vec!["All".to_string()]),
        ..Default::default()
    };

    let response = client.receive_message(req).await;
    for msg in response
        .expect("Expected to have a receive message response")
        .messages
        .expect("message should be available")
    {
        println!(
            "Received message '{}' with id {}",
            msg.body.clone().unwrap(),
            msg.message_id.clone().unwrap()
        );
        println!("Receipt handle is {:?}", msg.receipt_handle);

        delete_message(&client, queue_url.clone(), msg.receipt_handle.clone().unwrap()).await;
    }
}

async fn delete_message(client: &SqsClient, queue_url: &String, receipt_handle: &String) {

    let req = DeleteMessageRequest {
        queue_url: queue_url.clone(),
        receipt_handle: receipt_handle.clone()
    };

    match client.delete_message(req).await {
        Ok(_) => println!("Deleted message via receipt handle {:?}", receipt_handle.clone()),
        Err(e) => panic!("Couldn't delete message: {:?}", e),
    }
}

async fn send_message(client: &SqsClient, queue_url: &String, msg: &String) {

    let req = SendMessageRequest {
        message_body: msg.clone(),
        queue_url: queue_url.clone(),
        ..Default::default()
    };

    let response = client.send_message(req).await;

    println!(
        "Sent message with body '{}' and created message_id {}",
        msg,
        response.unwrap().message_id.unwrap()
    );
}

async fn list_queues(client: &SqsClient) {
    let req = ListQueuesRequest {
        ..Default::default()
    };

    let result = client.list_queues(req).await;

    println!("{:#?}", result);
}

async fn get_queue_url(client: &SqsClient) -> String {
    let req = GetQueueUrlRequest {
        queue_name: QUEUE_NAME.to_string(),
        ..Default::default()
    };

    let response = client
        .get_queue_url(req)
        .await
        .expect("Get queue by URL request failed");

    let queue_url = response
        .queue_url
        .expect("Queue url should be available from list queues");

    println!(
        "Verified queue url {} for queue name {}",
        queue_url.clone(),
        QUEUE_NAME
    );

    queue_url
}

async fn get_queue_attributes(client: &SqsClient, queue_url: &String) {
    let req = GetQueueAttributesRequest {
        queue_url: queue_url.clone(),
        attribute_names: Some(vec!["All".to_string()]),
    };

    match client.get_queue_attributes(req).await {
        Ok(result) => println!("Queue attributes: {:?}", result),
        Err(e) => panic!("Error getting queue attributes: {:?}", e),
    }
}

// fn list_queues() {
//     let client = SqsClient::new(Region::UsEast1);
//     let req: ListQueuesRequest = Default::default();

//     match client.list_queues(req).sync() {
//         Ok(output) => match output.queue_urls {
//             Some(queues) => {
//                 println!("Queues:");
//                 for queue in queues {
//                     println!("{}", queue);
//                 }
//             }
//             None => println!("No queues found!"),
//         },
//         Err(error) => {
//             println!("Error: {:?}", error);
//         }
//     }
//     // let request = rusoto_sqs::ListQueuesRequest {
//     //     ..Default::default()
//     // };

//     // let result = sqs.list_queues(request).await.expect("List queues failed");
//     // println!("{:#?}", result);
// }
