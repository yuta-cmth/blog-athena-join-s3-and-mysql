import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as athena from "aws-cdk-lib/aws-athena";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as rds from "aws-cdk-lib/aws-rds";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as glue from "aws-cdk-lib/aws-glue";
import * as serverless from "aws-cdk-lib/aws-sam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as logs from "aws-cdk-lib/aws-logs";
import * as iam from "aws-cdk-lib/aws-iam";
import { CdkResourceInitializer } from "./resource-initializer/resource-initializer";

export class BlogAthenaJoinS3AndMysqlStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    // Create a VPC
    const vpc = new ec2.Vpc(this, "BlogAthenaJoinS3MySQLVPC", {
      maxAzs: 2,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: "Public",
          subnetType: ec2.SubnetType.PUBLIC,
        },
        {
          cidrMask: 24,
          name: "Isolated",
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
        },
      ],
    });
    // Without this, federated query lambda cannot access SecretsManager.
    vpc.addInterfaceEndpoint("SecretsManagerEndpoint", {
      service: ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
    });
    // Without this, federated query lambda cannot access S3.
    vpc.addGatewayEndpoint("S3GatewayEndpoint", {
      service: ec2.GatewayVpcEndpointAwsService.S3,
    });

    // Create an S3 bucket
    const bucket = new s3.Bucket(this, "BlogAthenaJoinS3MySQLBucket", {
      bucketName: "blog-athena-join-s3-mysql-bucket",
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const dbSecret = new rds.DatabaseSecret(
      this,
      "BlogAthenaJoinS3MySQLDatabaseSecret",
      {
        username: "admin",
      }
    );

    // Create an RDS MySQL database
    const rdsDbName = "blog_athena_join_s3_mysql";
    const database = new rds.DatabaseInstance(
      this,
      "BlogAthenaJoinS3MySQLDatabase",
      {
        engine: rds.DatabaseInstanceEngine.mysql({
          version: rds.MysqlEngineVersion.VER_8_0,
        }),
        instanceType: ec2.InstanceType.of(
          ec2.InstanceClass.BURSTABLE4_GRAVITON,
          ec2.InstanceSize.MICRO
        ),
        allocatedStorage: 20,
        maxAllocatedStorage: 20,
        databaseName: rdsDbName,
        credentials: rds.Credentials.fromSecret(dbSecret),
        vpc,
        vpcSubnets: {
          subnetType: ec2.SubnetType.PUBLIC,
        },
        publiclyAccessible: true,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }
    );

    const mysqlConnectorSecurityGroup = new ec2.SecurityGroup(
      this,
      "BlogAthenaJoinS3MySQLConnectorSecurityGroup",
      {
        vpc: vpc,
      }
    );
    // Allow from lambda security group
    database.connections.allowFrom(
      mysqlConnectorSecurityGroup,
      ec2.Port.tcp(Number(database.instanceEndpoint.port))
    );

    const mysqlConnectorLambdaName = "athena-mysql-connector";
    // MySQL connector used in Athena federated query
    const mysqlConnector = new serverless.CfnApplication(
      this,
      "BlogAthenaJoinS3MySQLConnector",
      {
        location: {
          applicationId:
            "arn:aws:serverlessrepo:us-east-1:292517598671:applications/AthenaMySQLConnector",
          semanticVersion: "2024.15.2",
        },
        parameters: {
          LambdaFunctionName: mysqlConnectorLambdaName,
          DefaultConnectionString: `mysql://jdbc:mysql://${database.dbInstanceEndpointAddress}:${database.dbInstanceEndpointPort}/blog_athena_join_s3_mysql?\${${dbSecret.secretName}}`,
          SecretNamePrefix: "BlogAthenaJoinS3MySQL",
          SpillBucket: bucket.bucketName,
          SpillPrefix: "athena-mysql-connector-spill",
          LambdaTimeout: "30",
          LambdaMemory: "3008",
          DisableSpillEncryption: "false",
          SecurityGroupIds: mysqlConnectorSecurityGroup.securityGroupId,
          SubnetIds: vpc.isolatedSubnets
            .map((subnet) => subnet.subnetId)
            .join(","),
        },
      }
    );

    const athenaDataCatalogMySQL = new athena.CfnDataCatalog(
      this,
      "BlogAthenaJoinS3MySQLAthenaDataCatalog",
      {
        name: "BlogAthenaJoinS3MySQLAthenaDataCatalog",
        type: "LAMBDA",
        parameters: {
          function: `arn:aws:lambda:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:function:${mysqlConnectorLambdaName}`,
        },
      }
    );

    // Create a table and insert initial data.
    this.initializeRds({ dbSecret, vpc, database });

    // Attached to the Glue Crawler Role to crawl the S3 bucket
    const crawlerRole = new iam.Role(this, "BlogGlueCrawlerRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSGlueServiceRole"
        ),
      ],
      inlinePolicies: {
        glueCrawlerPolicy: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: ["s3:GetObject", "s3:PutObject"],
              resources: [`${bucket.bucketArn}/data/*`],
            }),
          ],
        }),
      },
    });

    const databaseName = "blog_athena_join_s3_mysql_db";
    // Create Glue Database
    new glue.CfnDatabase(this, "BlogAthenaJoinS3MySQLGlueDatabase", {
      databaseInput: {
        name: databaseName,
        description: "Blog Glue Database",
      },
      // CatalogId is the "AWS account ID for the account in which to create the catalog object".
      // https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-glue-database.html
      catalogId: this.account,
    });

    const workGroup = new athena.CfnWorkGroup(
      this,
      "BlogAthenaJoinS3MySQLWorkGroup",
      {
        name: "blog_athena_work_group",
        description: "Blog Athena Work Group",
        state: "ENABLED",
        recursiveDeleteOption: true,
        workGroupConfiguration: {
          resultConfiguration: {
            outputLocation: `s3://${bucket.bucketName}/query-results/`,
          },
        },
      }
    );

    const savedQueryCreate = new athena.CfnNamedQuery(
      this,
      "BlogAthenaJoinS3MySQLSavedQueryCreate",
      {
        database: databaseName,
        name: "blog_athena_saved_query_create",
        queryString: `CREATE EXTERNAL TABLE ${databaseName}.weather_data (
  year INT,
  city STRING,
  temperature INT,
  humidity INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'delimiter' = ',',
  'skip.header.line.count' = '1'
)
LOCATION 's3://${bucket.bucketName}/data';
`,
        description: "Create blog_data table",
        workGroup: workGroup.name,
      }
    );

    const savedQuerySelect = new athena.CfnNamedQuery(
      this,
      "BlogAthenaJoinS3MySQLSavedQuerySelect",
      {
        database: databaseName,
        name: "blog_athena_saved_query_select",
        queryString: `select year,
	m.name,
	temperature,
	humidity
from "${databaseName}"."weather_data" as w
	INNER JOIN "lambda:${mysqlConnectorLambdaName}"."${rdsDbName}"."master_dimension" as m ON w.city = m.key
order by year, m.name;`,
        description: "Select all from blog_data table",
        workGroup: workGroup.name,
      }
    );
    savedQueryCreate.addDependency(workGroup);
    savedQuerySelect.addDependency(workGroup);

    new cdk.CfnOutput(this, "BucketName", {
      value: bucket.bucketName,
    });
  }

  private initializeRds({
    dbSecret,
    vpc,
    database,
  }: {
    dbSecret: rds.DatabaseSecret;
    vpc: ec2.IVpc;
    database: rds.IDatabaseInstance;
  }) {
    const sg = new ec2.SecurityGroup(this, "RdsInitFnSg", {
      vpc,
      allowAllOutbound: true,
    });

    // Initializer function to create a table and insert initial data.
    const initializer = new CdkResourceInitializer(this, "MyRdsInit", {
      config: {
        credsSecretName: dbSecret.secretName,
      },
      fnLogRetention: logs.RetentionDays.ONE_WEEK,
      fnCode: lambda.DockerImageCode.fromImageAsset(
        `${__dirname}/resource-initializer/rds-init-fn-code`,
        {}
      ),
      fnTimeout: cdk.Duration.minutes(2),
      fnSecurityGroups: [sg],
      vpc,
      subnetsSelection: vpc.selectSubnets({
        subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
      }),
    });

    // Allow access from initializer.function to the RDS instance.
    database.connections.allowFrom(
      initializer.function,
      ec2.Port.tcp(Number(database.instanceEndpoint.port))
    );

    // Run the initializer function after the database is created.
    initializer.customResource.node.addDependency(database);
    // Allow the initializer function to read the RDS instance credentials secret.
    dbSecret.grantRead(initializer.function);
  }
}
