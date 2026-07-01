ET 200M → Profibus DP → CPU 315 2 PN/DP → Profinet (Industrial Ethernet)

Abstração de dados:
Edge Node (Docker) rodando um serviço Python com python snap7 lendo DBs/áreas do S7 300

Persistência bruta:
Banco SQL Server (camada RAW industrial)

Orquestração:
Airflow orquestrando ETLs (RAW → CURATED → GOLD).

1️⃣ Habilitar o CLP para leitura via S7 Protocol
No TIA Portal/Step7 (para a CPU 315 2 PN/DP):
•	Habilitar PUT/GET para acesso externo (S7 Communication)
•	Desabilitar Optimized Block Access nos DBs que você quer ler (para S7 300 isso já é “clássico”, mas vale garantir a compatibilidade de endereçamento).
•	Definir DBs bem estruturados para os Cozedores (por fluxo, por cozedor ou por grupo de variáveis).

Exemplo de organização:
•	DB100 → Cozedores Fluxo 1
•	DB200 → Cozedores Fluxo 2

2️⃣ Estratégia de abstração dos dados
Por DB estruturado (recomendado)
Você cria DBs com estrutura clara (REAL, INT, BOOL, etc.) e lê blocos de bytes via snap7, convertendo para tipos Python.
