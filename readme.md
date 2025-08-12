# Automação de Sincronização SAP-Excel para Power BI

Este projeto consiste em um serviço de sincronização robusto, desenvolvido em Python, que extrai dados de um banco de dados SAP HANA e os salva em arquivos `.xlsx`. O objetivo principal é automatizar a atualização de fontes de dados para dashboards do Power BI, garantindo que as informações estejam sempre atualizadas de forma eficiente e segura.

## Funcionalidades Principais

  - **Extração de Dados do SAP HANA**: Conecta-se de forma segura ao banco de dados e executa consultas SQL customizáveis.
  - **Agendamento Flexível**: Permite a execução de tarefas baseada tanto em **intervalos de tempo** (ex: a cada 5 minutos) quanto em **horários fixos** (ex: às 08:00, 12:30 e 17:00).
  - **Processamento Eficiente de Grandes Volumes**: Utiliza uma abordagem de *chunking* (processamento em lotes) para ler e escrever grandes volumes de dados sem sobrecarregar a memória do sistema.
  - **Segurança de Credenciais**: As credenciais do SAP não são armazenadas em texto plano. Elas são buscadas do Firebase Firestore e descriptografadas em tempo de execução usando uma chave secreta local.
  - **Controle de Concorrência**: Implementa um mecanismo de semáforo (lock) para garantir que apenas um ciclo de processamento de tarefas execute por vez, evitando sobreposições e condições de corrida.
  - **Manuseio Atômico de Arquivos**: Garante que o arquivo `.xlsx` final só seja substituído se todo o processo de gravação for bem-sucedido, prevenindo arquivos corrompidos.
  - **Formatação de Dados**: Aplica formatações de tipo de dado (texto, número, inteiro, data) nas colunas do DataFrame e do arquivo Excel final, garantindo a compatibilidade com o Power BI.
  - **Logging Detalhado**: Fornece logs claros sobre as operações, agendamentos, sucessos e falhas, facilitando a monitoria e a depuração.

## Estrutura do Projeto

O projeto é organizado nos seguintes módulos para uma clara separação de responsabilidades:

```
/
├── config/
│   ├── credentials.py        # Lógica para obter e descriptografar credenciais do Firebase.
│   └── settings.py           # Configurações globais (janela de execução, nomes de arquivos).
├── processing/
│   ├── dataframe_handler.py  # Aplica formatação de tipos de dados ao DataFrame pandas.
│   └── excel_writer.py       # Funções para salvar DataFrames em arquivos .xlsx de forma segura.
├── sap/
│   └── connection.py         # Módulo para conectar e executar consultas no SAP HANA.
├── utils/
│   └── scheduler.py          # Funções auxiliares para a lógica de agendamento global.
├── sap_sync_main.py          # O coração da aplicação, contendo o loop principal.
├── tarefas.json              # Arquivo de configuração para definir as tarefas de extração.
├── secret.key                # Chave de criptografia (NÃO DEVE SER VERSIONADA).
├── *.json                    # Arquivo de serviço do Firebase (NÃO DEVE SER VERSIONADO).
└── .gitignore                # Define os arquivos a serem ignorados pelo Git.
```

## Configuração

### `tarefas.json`

Este é o arquivo central para configurar todas as tarefas de extração. É uma lista de objetos JSON, onde cada objeto representa uma tarefa com as seguintes chaves:

  - `"consulta_sap"`: A consulta SQL exata a ser executada no SAP HANA.
  - `"tabela"`: Um nome curto para a tarefa, usado para nomear o arquivo de saída (ex: `"analise_venda"` gera `analise_venda.xlsx`).
  - `"intervalo"`: (Opcional) Frequência de execução em segundos. Usado se `horarios_execucao` não for definido.
  - `"horarios_execucao"`: (Opcional) Uma lista de horários fixos no formato `"HH:MM"` para a execução da tarefa. Se presente, ignora a chave `"intervalo"`.
  - `"chunk_size"`: (Opcional) O número de linhas a serem processadas por lote. Essencial para consultas muito grandes. O padrão é 10000.
  - `"colunas"`: (Opcional) Uma lista com a ordem exata das colunas desejadas no arquivo final.
  - `"xlsx_options"`: Um objeto para forçar a formatação de tipo em colunas específicas, contendo as chaves:
      - `"force_text"`
      - `"force_numeric"`
      - `"force_integer"`
      - `"force_date"`

### Arquivos de Credenciais

  - **`secret.key`**: Um arquivo que contém a chave de criptografia Fernet. Este arquivo é gerado separadamente e deve ser mantido em segredo.
  - **`*.json` (Firebase Admin SDK)**: O arquivo JSON de credenciais para a conta de serviço do Firebase, obtido no console do Firebase.

## Lógica de Execução e Detalhes Técnicos

### Fluxo Principal (`sap_sync_main.py`)

O script opera em um loop infinito (`while True`) que coordena todo o processo:

1.  **Obtenção de Credenciais**: No início, busca e descriptografa as credenciais do SAP uma única vez.
2.  **Verificação do Lock (Semáforo)**: Tenta adquirir um `threading.Lock`. Se o lock já estiver em uso, significa que um ciclo anterior ainda está em andamento, então o script aguarda 1 segundo e tenta novamente. Isso previne a sobreposição de execuções.
3.  **Atualização de Tarefas**: Periodicamente, recarrega o arquivo `tarefas.json` para permitir a alteração das configurações sem precisar reiniciar o serviço.
4.  **Verificação da Janela de Execução**: Confere se o horário atual está dentro da janela de operação global definida em `config/settings.py` (`HORARIO_PERMITIDO`). Se estiver fora, ele "dorme" até o início da próxima janela.
5.  **Processamento de Tarefas**: Itera sobre a lista de tarefas ativas e verifica se o tempo de `proxima_execucao` de alguma delas já foi alcançado.
6.  **Agendamento da Próxima Execução**: Após uma tarefa ser concluída com sucesso, sua próxima execução é recalculada com base na sua configuração (`intervalo` ou `horarios_execucao`). Em caso de falha, a tarefa é reagendada para uma nova tentativa em um curto intervalo de tempo (`ERROR_RETRY_INTERVAL`).
7.  **Liberação do Lock**: Ao final do ciclo de verificação, o lock é liberado no bloco `finally`, garantindo que ele fique disponível para a próxima iteração.

### Segurança e Criptografia (`config/credentials.py`)

Para evitar credenciais expostas no código, o sistema utiliza um processo de duas camadas:

1.  As credenciais do SAP (usuário, senha, host) são armazenadas de forma criptografada no **Firebase Firestore**.
2.  O script `credentials.py` lê o arquivo `secret.key` local, se conecta ao Firebase, busca os dados criptografados e os descriptografa usando a chave lida, tornando-os disponíveis para a conexão com o SAP.

### Processamento de Dados em Lotes (Chunking)

Para lidar com consultas que retornam milhões de linhas, a aplicação evita carregar todos os dados na memória.

1.  A função `executar_consulta_em_chunks` em `sap/connection.py` usa `cursor.fetchmany(chunk_size)` para buscar os dados do banco em lotes. Ela usa `yield` para funcionar como um gerador, entregando um lote de cada vez.
2.  Em `sap_sync_main.py`, a função `processar_tarefa` itera sobre esses lotes. Cada lote é formatado e passado para a função de escrita.
3.  A função `salvar_xlsx_em_chunks_atomic` em `processing/excel_writer.py` recebe esses lotes e os anexa a um arquivo temporário. Somente ao final do processo o arquivo temporário substitui o arquivo final.

## Como Executar o Projeto

### Pré-requisitos

  - Python 3.x
  - Bibliotecas Python: `pandas`, `openpyxl`, `hdbcli`, `firebase-admin`, `cryptography`. Instale-as com:
    ```bash
    pip install pandas openpyxl hdbcli firebase-admin cryptography
    ```

### Setup

1.  **Firebase**: Configure um projeto no Firebase Firestore e insira as credenciais do SAP criptografadas.
2.  **Credenciais**: Coloque o arquivo `*.json` da conta de serviço do Firebase na pasta raiz do projeto e configure o nome correto na variável `FIREBASE_CRED_JSON` em `config/settings.py`.
3.  **Chave Secreta**: Gere e salve sua chave de criptografia Fernet no arquivo `secret.key`.
4.  **Tarefas**: Configure suas consultas e agendamentos no arquivo `tarefas.json`.

### Execução

Para iniciar o serviço de sincronização, execute o script principal a partir do seu terminal:

```bash
python sap_sync_main.py
```

O serviço começará a rodar, exibindo os logs de suas atividades no console. Para encerrar, pressione `CTRL+C`.

## Versionamento (`.gitignore`)

O arquivo `.gitignore` está configurado para impedir que arquivos sensíveis ou desnecessários sejam enviados para o repositório Git. Isso inclui:

  - Arquivos de credenciais (`*.json`, `secret.key`).
  - Arquivos `.xlsx` gerados.
  - Pastas de cache do Python (`__pycache__`).