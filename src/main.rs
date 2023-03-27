use std::str;
use std::error::Error;
use std::io::{Read, Write};
use std::fs::File;
use std::io::Seek;
use std::path::Path;
use tokio::io::copy;

use aws_sdk_s3::{Client, Region};
use aws_sdk_s3::error::{CreateBucketError, GetObjectError, PutObjectError};
use aws_sdk_s3::output::{GetObjectOutput, PutObjectOutput};
use aws_sdk_s3::types::{ByteStream, SdkError};

use clap::Parser;
use tempfile::NamedTempFile;
use walkdir::{DirEntry, WalkDir};

use zip::{ZipArchive, ZipWriter};
use zip::result::ZipError;
use zip::write::FileOptions;

#[derive(Parser, Debug)]
enum Args {
    #[command(name = "backup", about = "backup a directory")]
    Backup(BackupParams),

    #[command(name = "restore", about = "restore files")]
    Restore(RestoreParams),
}

#[derive(Parser, Debug)]
struct BackupParams {
    #[arg(short = 'p')]
    path: String,
    #[arg(short = 'b')]
    bucket: String,
    #[arg(short = 'k')]
    key: String,
}

#[derive(Parser, Debug)]
struct RestoreParams {
    #[arg(short = 'p')]
    path: String,
    #[arg(short = 'b')]
    bucket: String,
    #[arg(short = 'k')]
    key: String,
    #[arg(short = 'f')]
    file: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    match Args::parse() {
        Args::Backup(params) => backup(params).await,
        Args::Restore(params) => restore(params).await,
    }
}

async fn backup(BackupParams { path, bucket, key }: BackupParams) -> Result<(), Box<dyn Error>> {
    let client = get_client().await;
    create_bucket_if_not_exists(&client, &bucket).await?;

    let mut temp = NamedTempFile::new()?;
    zip_dir(&path, &mut temp, zip::CompressionMethod::Deflated)?;

    upload_object(&client, &bucket, temp.path(), &key).await?;

    Ok(())
}

async fn restore(RestoreParams { path, bucket, key, file }: RestoreParams) -> Result<(), Box<dyn Error>> {
    let client = get_client().await;

    let stream = download_object(&client, &bucket, &key).await?.body;
    let temp = NamedTempFile::new()?;
    let mut temp2 = temp.reopen()?;
    let mut reader = stream.into_async_read();
    let mut temp = tokio::fs::File::from_std(temp.into_file());
    copy(&mut reader, &mut temp).await?;

    let mut archive = ZipArchive::new(&mut temp2)?;
    match file {
        None => archive.extract(path)?,
        Some(file) => {
            let mut output = File::create(Path::new(&path).join(&file))?;
            let mut file = archive.by_name(&file)?;
            std::io::copy(&mut file, &mut output)?;
        }
    }
    Ok(())
}

async fn get_client() -> Client {
    let region = Region::new("us-east-1");
    let shared_config = aws_config::from_env().region(region.clone()).load().await;
    Client::new(&shared_config)
}

pub async fn download_object(
    client: &Client,
    bucket_name: &str,
    key: &str,
) -> Result<GetObjectOutput, SdkError<GetObjectError>> {
    client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
}

pub async fn upload_object(
    client: &Client,
    bucket_name: &str,
    file_name: &Path,
    key: &str,
) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
    let body = ByteStream::from_path(file_name).await;
    client
        .put_object()
        .bucket(bucket_name)
        .key(key)
        .body(body.unwrap())
        .send()
        .await
}

pub async fn create_bucket_if_not_exists(
    client: &Client,
    bucket_name: &str,
) -> Result<(), SdkError<CreateBucketError>> {
    if let Ok(_) = client.head_bucket().bucket(bucket_name).send().await {
        return Ok(());
    }
    client
        .create_bucket()
        .bucket(bucket_name)
        .send()
        .await
        .map(drop)
}

fn zip_dir<T: Write + Seek>(src: &str, dst: T, method: zip::CompressionMethod) -> zip::result::ZipResult<()> {
    fn helper<T: Write + Seek, I: Iterator<Item=DirEntry>>(
        it: &mut I,
        prefix: &str,
        writer: T,
        method: zip::CompressionMethod
    ) -> zip::result::ZipResult<()> {
        let mut zip = ZipWriter::new(writer);
        let options = FileOptions::default()
            .compression_method(method)
            .unix_permissions(0o755);

        let mut buffer = Vec::new();
        for entry in it {
            let path = entry.path();
            let name = path.strip_prefix(Path::new(prefix)).unwrap();

            if path.is_file() {
                zip.start_file(name.to_str().unwrap(), options)?;
                let mut f = File::open(path)?;

                f.read_to_end(&mut buffer)?;
                zip.write_all(&*buffer)?;
                buffer.clear();
            } else if name.as_os_str().len() != 0 {
                zip.add_directory(name.to_str().unwrap(), options)?;
            }
        }
        zip.finish()?;
        Ok(())
    }

    if !Path::new(src).is_dir() {
        return Err(ZipError::FileNotFound);
    }

    let walkdir = WalkDir::new(src.to_string());
    let it = walkdir.into_iter();

    helper(&mut it.filter_map(|e| e.ok()), src, dst, method)?;

    Ok(())
}
