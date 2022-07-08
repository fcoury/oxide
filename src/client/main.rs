use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use mongodb::Client;
use std::env;
use std::error::Error;
use tokio;

async fn _find(client: &Client) -> Result<(), Box<dyn Error>> {
    let filter = doc! { "x": 1 };
    let find_options = mongodb::options::FindOptions::builder()
        .sort(doc! { "x": -1 })
        .build();
    let mut cursor = client
        .database("test")
        .collection::<Document>("col")
        .find(filter, find_options)
        .await?;

    while let Some(result) = cursor.try_next().await? {
        println!("{:?}", result);
    }

    Ok(())
}

async fn list(client: &Client) -> Result<(), Box<dyn Error>> {
    println!("Databases:");
    for name in client.list_database_names(None, None).await? {
        println!("- {}", name);
    }

    println!("\nDatabase infos:");
    for db in client.list_databases(None, None).await? {
        println!("- {:?}", db);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Load the MongoDB connection string from an environment variable:
    let client_uri =
        env::var("MONGODB_URI").expect("You must set the MONGODB_URI environment var!");

    // A Client is needed to connect to MongoDB:
    let client = Client::with_uri_str(&client_uri).await?;

    // Print the databases in our MongoDB cluster:
    // println!("Databases:");
    // for name in client.list_database_names(None, None).await? {
    //     println!("- {}", name);
    // }

    // println!("\nDatabase infos:");
    // for db in client.list_databases(None, None).await? {
    //     println!("- {:?}", db);
    // }

    // let docs = vec![doc! {"x": 1}, doc! {"x": 2}];
    // client
    //     .database("test")
    //     .collection::<Document>("col")
    //     .insert_many(docs, None)
    //     .await?;

    // find(&client).await?;

    list(&client).await?;

    Ok(())
}
