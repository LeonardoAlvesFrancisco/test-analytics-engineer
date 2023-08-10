DESCRIÇÃO

Esse case é composto de dois datasets no formato csv: ProdutosRetailers.csv e Coletas.csv.

O dataset ProdutosRetailers contém os dados com as informações de produtos e seus fabricantes, bem como dos marketplaces.

Já, o database Coletas.csv contém os dados de coleta de produtos em vários marketplaces diariamente.

CAMPOS:
	
ProdutosRetailer:
	.Customer   - Fabricante do produto
	.Department - Departamento do produto
	.Category   - Categoria do produto
	.Brand      - Marca do produto
	.EAN        - Código de identificação do produto
	.Product    - Nome do produto
	.Retailer   - Nome do marketplace
	.MasterKey_RetailerProduct - Chave que considera o marketplace e o produto

Coletas:
	.DateIns                   - Data de coleta
	.Screenshot                - Screenshot do produto coletado
	.Available                 - Indica se o produto está disponível ou não (1 - disponível; 0 - não disponível)
	.Unavailable               - Indica se o produto não está disponível (1 - não disponível; 0 disponível)
	.SuggestedPrice            - Preço Sugerido de venda para o produto
	.FinalPrice                - Preço de venda coletado no marketplace
	.FromPrice                 - Preço de venda antes do desconto
	.MasterKey_RetailerProduct - Chave que considera o marketplace e o produto


OBJETIVO:

	.Realizar o data cleaning nos dois datasets.
	.Extrair insights.
	.Implementação de modelo de ML (Plus)


PREMISSAS:

	.MasterKey_RetailerProduct do database ProdutosRetailers.csv é único.
	.MasterKey_RetailerProduct do database ProdutosRetailers.csv é único para cada dia.
	.As coletas são realizadas diariamente, impreterivelmente.

FERRAMENTAS:

	.Utilizar Python ou R.
	.Disponibilizar os resultados no Github pessoal (apresentação, notebook, código, etc...)
	.Pode-se utilizar outras ferramentas de visualização caso seja necessário.