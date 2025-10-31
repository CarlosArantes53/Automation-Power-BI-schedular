---

# **Guia de Configuração: tarefas.json**

O arquivo tarefas.json é o coração do serviço de automação. Ele define *quais* dados extrair do SAP, *para onde* enviá-los e *com que frequência* o processo deve ser executado.

O arquivo consiste em uma **lista de objetos JSON**, onde cada objeto representa uma única tarefa de extração.

JSON

\[  
  {  
    "tabela": "nome\_da\_tarefa\_1",  
    "consulta\_sap": "SELECT ...",  
    ...  
  },  
  {  
    "tabela": "nome\_da\_tarefa\_2",  
    "consulta\_sap": "SELECT ...",  
    ...  
  }  
\]

## **Chaves de Configuração por Tarefa**

Cada objeto de tarefa na lista pode conter as seguintes chaves:

| Chave | Obrigatório? | Descrição |
| :---- | :---- | :---- |
| **tabela** | **Sim** | Nome de destino para os dados. **Importante:** Se o formato for xlsx, este será o nome da **planilha (aba)**. Se for db, será o nome da **tabela** no banco de dados. |
| **consulta\_sap** | **Sim** | A consulta SQL exata a ser executada no SAP HANA. |
| **formato\_saida** | Não | O formato do arquivo final. Padrão: "xlsx". Valores suportados: "xlsx", "db", "csv", "parquet". |
| **arquivo\_saida** | Não | O caminho do arquivo de destino (ex: "relatorios/dados.db"). Se omitido, o nome do arquivo será gerado a partir da chave tabela (ex: "nome\_da\_tarefa\_1.xlsx"). |
| **horarios\_execucao** | Não | Uma lista de horários fixos para execução, no formato "HH:MM". Ex: \["08:00", "12:30", "17:00"\]. **Esta chave tem prioridade sobre intervalo**. |
| **intervalo** | Não | Frequência de execução em segundos. Ex: 300 (para 5 minutos). Usado apenas se horarios\_execucao não for definido. Padrão: 300 segundos. |
| **chunk\_size** | Não | O número de linhas a serem processadas por lote (chunk). Essencial para consultas muito grandes. Padrão: 10000\. |
| **colunas** | Não | Uma lista com a ordem exata das colunas desejadas no arquivo final. Se uma coluna da lista não existir na consulta, ela será criada com valores nulos. |
| **xlsx\_options** | Não | Um objeto para forçar a formatação de tipo em colunas específicas (veja dataframe\_handler.py). Chaves suportadas: force\_text, force\_numeric, force\_integer, force\_date. |

---

## **Regras de Agendamento (Prioridade)**

O sistema decide quando executar uma tarefa seguindo esta ordem:

1. **horarios\_execucao:** Se esta chave existir e contiver uma lista de horários (ex: \["09:00"\]), a tarefa SÓ será executada nesses horários. A chave intervalo será ignorada.  
2. **intervalo:** Se horarios\_execucao *não* for definido, o sistema usará o valor de intervalo (em segundos) para agendar a próxima execução após a conclusão da atual.  
3. **Padrão (Nenhum dos dois):** Se nem horarios\_execucao nem intervalo forem definidos, a tarefa executará imediatamente na primeira vez e, após a conclusão, usará o intervalo padrão de **300 segundos** (5 minutos) para as próximas execuções.

---

## **Exemplos de Configuração**

### **Cenário 1: Tarefa Simples (Execução por Intervalo)**

Uma única consulta que roda a cada 10 minutos (600s) e salva em seu próprio arquivo Excel.

JSON

\[  
  {  
    "tabela": "analise\_vendas\_diarias",  
    "consulta\_sap": "SELECT CardCode, DocNum, DocDate, DocTotal FROM OINV WHERE DocDate \= CURRENT\_DATE",  
    "intervalo": 600,  
    "formato\_saida": "xlsx"  
  }  
\]

* **Resultado:** Será criado o arquivo analise\_vendas\_diarias.xlsx (porque arquivo\_saida foi omitido). A planilha interna (aba) se chamará analise\_vendas\_diarias (definido pela tabela).

### **Cenário 2: Tarefa Agendada (Horários Fixos)**

Uma consulta que roda três vezes ao dia em horários específicos.

JSON

\[  
  {  
    "tabela": "relatorio\_financeiro\_fechamento",  
    "consulta\_sap": "SELECT ... FROM JDT1 WHERE ...",  
    "horarios\_execucao": \["08:30", "12:30", "18:00"\],  
    "formato\_saida": "xlsx"  
  }  
\]

* **Resultado:** A tarefa rodará pontualmente às 08:30, 12:30 e 18:00 (dentro da janela de operação global). O intervalo é ignorado.

### **Cenário 3: Agrupamento de Arquivos (Múltiplas Tarefas, Um Arquivo)**

Este é o cenário mais avançado, onde múltiplas consultas salvam no mesmo arquivo de destino.

#### **Exemplo 3a: Agrupamento em Banco de Dados (SQLite)**

Duas tarefas que salvam no **mesmo arquivo** processo.db, mas em **tabelas diferentes** (conforme seu exemplo).

JSON

\[  
  {  
    "tabela": "tbl\_insumos",  
    "consulta\_sap": "SELECT itemcode, descricao, espessura\_bobina, largura\_bobina FROM TBL\_INSUMOS\_SAP",  
    "formato\_saida": "db",  
    "arquivo\_saida": "database/processo.db",  
    "horarios\_execucao": \["08:00", "14:00"\]  
  },  
  {  
    "tabela": "tbl\_demanda",  
    "consulta\_sap": "SELECT itemcode, descricao, espessura, demanda, estoque\_atual FROM TBL\_DEMANDA\_SAP",  
    "formato\_saida": "db",  
    "arquivo\_saida": "database/processo.db",  
    "horarios\_execucao": \["08:00", "14:00"\]  
  }  
\]

* **Resultado:** Ambas as tarefas atualizarão o arquivo database/processo.db. A primeira criará/substituirá a tabela tbl\_insumos, e a segunda criará/substituirá a tabela tbl\_demanda dentro do *mesmo* arquivo.

#### **Exemplo 3b: Agrupamento em Excel (Múltiplas Abas)**

Duas tarefas que salvam no **mesmo arquivo** relatorio\_powerbi.xlsx, mas em **planilhas (abas) diferentes**.

JSON

\[  
  {  
    "tabela": "Vendas\_Por\_Linha",  
    "consulta\_sap": "SELECT ItmsGrpCod, SUM(LineTotal) FROM INV1 ... GROUP BY ItmsGrpCod",  
    "formato\_saida": "xlsx",  
    "arquivo\_saida": "relatorios/relatorio\_powerbi.xlsx",  
    "intervalo": 300  
  },  
  {  
    "tabela": "Estoque\_Atual",  
    "consulta\_sap": "SELECT ItemCode, WhsCode, OnHand FROM OITW WHERE OnHand \> 0",  
    "formato\_saida": "xlsx",  
    "arquivo\_saida": "relatorios/relatorio\_powerbi.xlsx",  
    "intervalo": 300  
  }  
\]

* **Resultado:** O arquivo relatorios/relatorio\_powerbi.xlsx conterá duas abas: Vendas\_Por\_Linha e Estoque\_Atual, ambas atualizadas a cada 5 minutos.

### **Cenário 4: Formatação Avançada e Outros Formatos**

Uso de xlsx\_options e formatos csv / parquet.

JSON

\[  
  {  
    "tabela": "forcar\_formatacao",  
    "consulta\_sap": "SELECT ItemCode, DocDate, Quantity, Price FROM ...",  
    "formato\_saida": "xlsx",  
    "arquivo\_saida": "formatado.xlsx",  
    "intervalo": 900,  
    "colunas": \[  
      "ItemCode",  
      "DocDate",  
      "Quantity",  
      "Price",  
      "Coluna\_Extra\_Vazia"  
    \],  
    "xlsx\_options": {  
      "force\_text": \["ItemCode"\],  
      "force\_date": \["DocDate"\],  
      "force\_integer": \["Quantity"\],  
      "force\_numeric": \["Price"\]  
    }  
  },  
  {  
    "tabela": "dados\_para\_csv",  
    "consulta\_sap": "SELECT \* FROM ...",  
    "formato\_saida": "csv",  
    "intervalo": 3600  
  }  
\]

* **Resultado 1:** O arquivo formatado.xlsx terá a coluna ItemCode formatada como texto (preservando zeros à esquerda), DocDate como data, etc., e incluirá uma coluna extra vazia.  
* **Resultado 2:** O arquivo dados\_para\_csv.csv será gerado. **Nota:** Os formatos csv e parquet não suportam agrupamento; cada tarefa *sempre* substituirá o arquivo de destino.