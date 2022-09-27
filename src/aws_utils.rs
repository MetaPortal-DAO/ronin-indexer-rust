use aws_sdk_dynamodb::model::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ProvisionedThroughput,
    ScalarAttributeType, Select, TableStatus,
};
use aws_sdk_dynamodb::{Client, Error};
use aws_smithy_http::result::SdkError;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransferOnly {
    pub ts: String,
    pub block: String,
    pub from: String,
    pub to: String,
    pub value: String,
}

pub async fn does_table_exist(client: &Client, table: &str) -> Result<bool, Error> {
    let table_exists = client
        .list_tables()
        .send()
        .await
        .expect("should succeed")
        .table_names()
        .as_ref()
        .unwrap()
        .contains(&table.into());

    Ok(table_exists)
}

pub async fn create_table(client: &Client, table: &str, key: &str) -> Result<(), Error> {
    let a_name: String = key.into();
    let table_name: String = table.into();

    let ad = AttributeDefinition::builder()
        .attribute_name(&a_name)
        .attribute_type(ScalarAttributeType::S)
        .build();

    let ks = KeySchemaElement::builder()
        .attribute_name(&a_name)
        .key_type(KeyType::Hash)
        .build();

    let pt = ProvisionedThroughput::builder()
        .read_capacity_units(10)
        .write_capacity_units(5)
        .build();

    match client
        .create_table()
        .table_name(table_name)
        .key_schema(ks)
        .attribute_definitions(ad)
        .provisioned_throughput(pt)
        .send()
        .await
    {
        Ok(_) => println!("Added table {} with key {}", table, key),
        Err(e) => {
            println!("Got an error creating table:");
            println!("{}", e);
        }
    };

    Ok(())
}

pub async fn add_item(
    client: &Client,
    table: &str,
    item: TransferOnly,
) -> Result<(), SdkError<aws_sdk_dynamodb::error::PutItemError>> {
    let key = AttributeValue::S(Uuid::new_v4().to_string());
    let ts = AttributeValue::S(item.ts);
    let block = AttributeValue::S(item.block);
    let from = AttributeValue::S(item.from);
    let to = AttributeValue::S(item.to);
    let value = AttributeValue::S(item.value);

    match client
        .put_item()
        .table_name(table)
        .item("key", key)
        .item("ts", ts)
        .item("block", block)
        .item("to", to)
        .item("from", from)
        .item("value", value)
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
