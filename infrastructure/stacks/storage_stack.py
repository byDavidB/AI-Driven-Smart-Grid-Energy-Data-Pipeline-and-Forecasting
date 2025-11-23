from aws_cdk import (
    Stack,
    aws_s3 as s3,
    RemovalPolicy,
    Duration,
)
from constructs import Construct

class StorageStack(Stack):
    """
    StorageStack defines the S3 buckets for the Data Lake.
    
    Resources:
    - Raw Bucket: Ingested data (JSON), 30-day retention.
    - Curated Bucket: Processed data (Parquet).
    - Scripts Bucket: Glue scripts and artifacts.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        """
        Initialize the StorageStack.

        :param scope: The scope in which to define this construct.
        :param construct_id: The scoped construct ID.
        :param kwargs: Additional arguments.
        """
        super().__init__(scope, construct_id, **kwargs)

        # Raw Data Bucket
        # Lifecycle: Delete objects after 30 days
        self.raw_bucket = s3.Bucket(
            self,
            "GridRawBucket",
            bucket_name=f"grid-raw-{self.account}",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(30),
                    id="DeleteRawAfter30Days"
                )
            ]
        )

        # Curated Data Bucket
        self.curated_bucket = s3.Bucket(
            self,
            "GridCuratedBucket",
            bucket_name=f"grid-curated-{self.account}",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Scripts Bucket (for Glue jobs)
        self.scripts_bucket = s3.Bucket(
            self,
            "GridScriptsBucket",
            bucket_name=f"grid-scripts-{self.account}",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
