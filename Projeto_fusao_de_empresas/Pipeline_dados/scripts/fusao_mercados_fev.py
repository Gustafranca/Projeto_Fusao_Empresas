from processsamento_dados import Dados

#Extract

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

dados_empresaA = Dados.leituras_dados(path_json, 'json')
print('\n')
print(f'Nome das colunas empresa A : {dados_empresaA.nome_colunas} \n')
print(f'Quantidade de linhas no arquivo empresa A : {dados_empresaA.qtd_linhas}\n')

dados_empresaB = Dados.leituras_dados(path_csv, 'csv')
print(f'Nome das colunas empresa B : {dados_empresaB.nome_colunas}\n')
print(f'Quantidade de linhas no arquivo empresa B :{dados_empresaB.qtd_linhas}\n')

#Transform
#Para alterar o nome das coluna basta mudar os nomes dos values
#                                      |
#                                      v
key_mapping = {'Nome do Item' : 'Nome do Produto',
               'Classificação do Produto' : 'Categoria do Produto',
               'Valor em Reais (R$)' : 'Preço do Produto (R$)',
                'Quantidade em Estoque' : 'Quantidade em Estoque',
                'Nome da Loja' : 'Filial',
                'Data da Venda' : 'Data da Venda'} 

dados_empresaB.rename_columns(key_mapping)
print(f'**Novos** nomes para as colunas empresa B :{dados_empresaB.nome_colunas}\n')

dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print(f'Nome das colunas no arquivo após a fusao de empresas {dados_fusao.nome_colunas}\n')
print(f'Quantidade de linhas do arquivo após a fusao de empresas {dados_fusao.qtd_linhas}\n')

#Load

path_dados_combinados = 'data_processed/dados_combinados.csv'

dados_fusao.salvando_dados(path_dados_combinados)
print(f'Caminho do local onde o arquivo das empresas fundidas se encontra : {path_dados_combinados}\n')
