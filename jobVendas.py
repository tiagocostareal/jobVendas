import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node vendedores
vendedores_node1693760100644 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="vendedores_csv",
    transformation_ctx="vendedores_node1693760100644",
)

# Script generated for node Vendas
Vendas_node1693758897900 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="vendas_csv",
    transformation_ctx="Vendas_node1693758897900",
)

# Script generated for node intensvenda
intensvenda_node1693759354996 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="itensvenda_csv",
    transformation_ctx="intensvenda_node1693759354996",
)

# Script generated for node clientes
clientes_node1693759850210 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="clientes_csv",
    transformation_ctx="clientes_node1693759850210",
)

# Script generated for node produtos
produtos_node1693759990569 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="produtos_csv",
    transformation_ctx="produtos_node1693759990569",
)

# Script generated for node VendasMapping
VendasMapping_node1693759167951 = ApplyMapping.apply(
    frame=Vendas_node1693758897900,
    mappings=[
        ("idvenda", "long", "idvenda", "long"),
        ("idvendedor", "long", "idvendedor_vendas", "long"),
        ("idcliente", "long", "idcliente_vendas", "long"),
        ("data", "string", "data", "string"),
        ("total", "double", "total", "double"),
    ],
    transformation_ctx="VendasMapping_node1693759167951",
)

# Script generated for node itensvenda_mapping
itensvenda_mapping_node1693759405194 = ApplyMapping.apply(
    frame=intensvenda_node1693759354996,
    mappings=[
        ("idproduto", "long", "idproduto_itensvenda", "long"),
        ("idvenda", "long", "idvenda_itensvenda", "long"),
        ("quantidade", "long", "quantidade", "long"),
        ("valorunitario", "double", "valorunitario", "double"),
        ("valortotal", "double", "valortotal", "double"),
        ("desconto", "double", "desconto", "double"),
    ],
    transformation_ctx="itensvenda_mapping_node1693759405194",
)

# Script generated for node Joinvendas_itensvenda
Joinvendas_itensvenda_node1693759577012 = Join.apply(
    frame1=VendasMapping_node1693759167951,
    frame2=itensvenda_mapping_node1693759405194,
    keys1=["idvenda"],
    keys2=["idvenda_itensvenda"],
    transformation_ctx="Joinvendas_itensvenda_node1693759577012",
)

# Script generated for node JoinClientes
JoinClientes_node1693759890072 = Join.apply(
    frame1=clientes_node1693759850210,
    frame2=Joinvendas_itensvenda_node1693759577012,
    keys1=["idcliente"],
    keys2=["idcliente_vendas"],
    transformation_ctx="JoinClientes_node1693759890072",
)

# Script generated for node Joinprodutos
Joinprodutos_node1693760019580 = Join.apply(
    frame1=produtos_node1693759990569,
    frame2=JoinClientes_node1693759890072,
    keys1=["idproduto"],
    keys2=["idproduto_itensvenda"],
    transformation_ctx="Joinprodutos_node1693760019580",
)

# Script generated for node Joinvendedores
Joinvendedores_node1693760121969 = Join.apply(
    frame1=vendedores_node1693760100644,
    frame2=Joinprodutos_node1693760019580,
    keys1=["idvendedor"],
    keys2=["idvendedor_vendas"],
    transformation_ctx="Joinvendedores_node1693760121969",
)

# Script generated for node colunasfinais
colunasfinais_node1693760216852 = ApplyMapping.apply(
    frame=Joinvendedores_node1693760121969,
    mappings=[
        ("nome", "string", "nome", "string"),
        ("produto", "string", "produto", "string"),
        ("preco", "double", "preco", "double"),
        ("cliente", "string", "cliente", "string"),
        ("estado", "string", "estado", "string"),
        ("sexo", "string", "sexo", "string"),
        ("status", "string", "status", "string"),
        ("data", "string", "data", "string"),
        ("total", "double", "total", "double"),
        ("quantidade", "long", "quantidade", "long"),
        ("valorunitario", "double", "valorunitario", "double"),
        ("valortotal", "double", "valortotal", "double"),
        ("desconto", "double", "desconto", "double"),
    ],
    transformation_ctx="colunasfinais_node1693760216852",
)

# Script generated for node Datalake
Datalake_node1693760472425 = glueContext.write_dynamic_frame.from_options(
    frame=colunasfinais_node1693760216852,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://datalake5620/datalake/",
        "partitionKeys": ["status"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="Datalake_node1693760472425",
)

job.commit()
