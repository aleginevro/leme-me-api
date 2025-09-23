// server.js — LEME-ME API (VERSÃO OTIMIZADA PARA PERFORMANCE)
const express = require('express');
const sql = require('mssql');
const cors = require('cors');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;
const HOST = '0.0.0.0';

// *** MODIFICAÇÃO AQUI: CONFIGURAÇÃO EXPLÍCITA DO CORS ***
app.use(cors({
  origin: [
    'http://localhost:3000', // Para desenvolvimento local, se você usar
    'https://preview--sales-pulse-ee1f17bb.base44.app', // URL do seu preview Base44
    'https://sales-pulse-ee1f17bb.base44.app', // <<--- ADICIONEI ESTA LINHA COM O URL DO SEU APP PUBLICADO
    'https://base44.app',                              // Domínio principal do Base44
    'https://leme-me-api.onrender.com'                 // Adiciona a própria URL da API (por segurança)
  ],
  methods: ['GET', 'POST', 'PUT', 'DELETE'], // Adapte se usar outros métodos
  credentials: true // Se você planeja usar cookies ou autenticação baseada em credenciais
}));
// ******************************************************


app.use(express.json());

const dbConfig = {
  server: process.env.DB_HOST || process.env.DB_SERVER || 'fenixsys.emartim.com.br',
  port: parseInt(process.env.DB_PORT || '20902', 10),
  database: process.env.DB_NAME || process.env.DB_DATABASE || 'LevemeFenix',
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  options: {
    encrypt: (process.env.DB_ENCRYPT || 'false') === 'true',
    trustServerCertificate: (process.env.DB_TRUST_SERVER_CERTIFICATE || 'true') === 'true',
    enableArithAbort: true
  },
  pool: { 
    max: 10, 
    min: 0, 
    idleTimeoutMillis: 30000,
    acquireTimeoutMillis: 60000,
    createTimeoutMillis: 30000
  },
  connectionTimeout: 60000,
  requestTimeout: 120000 // 2 minutos
};

let pool = null;

async function connectWithRetry(retries = 10, delayMs = 5000) {
  for (let i = 1; i <= retries; i++) {
    try {
      pool = await sql.connect(dbConfig);
      console.log('✅ DB conectado');
      return pool;
    } catch (err) {
      console.error(`❌ Tentativa ${i} falhou: ${err.message}`);
      if (i === retries) {
        console.warn('⚠️ Não conectou ao DB; API segue online sem DB');
        return null;
      }
      await new Promise(r => setTimeout(r, delayMs));
    }
  }
}

async function getPool() {
  if (pool && pool.connected) return pool;
  try {
    pool = await sql.connect(dbConfig);
    return pool;
  } catch (err) {
    console.error('Falha ao obter pool de conexão:', err.message);
    return null;
  }
}

app.get('/', (req, res) => {
  res.send('API da LEME-ME funcionando! Conecte-se ao /status para verificar o DB.');
});

app.get('/status', async (req, res) => {
  const currentPool = await getPool();
  if (currentPool && currentPool.connected) {
    res.status(200).json({ status: 'API e DB conectados com sucesso!' });
  } else {
    res.status(500).json({ status: 'API funcionando, mas DB desconectado.' });
  }
});

// ENDPOINT OTIMIZADO PARA PERFORMANCE
app.get('/dashboard-data', async (req, res) => {
  try {
    const currentPool = await getPool();
    if (!currentPool || !currentPool.connected) {
      return res.status(503).json({ error: 'Serviço de banco de dados indisponível.' });
    }

    const request = currentPool.request();
    request.timeout = 120000; // 2 minutos de timeout

    console.log('🔄 Iniciando execução da query...');
    const startTime = Date.now();

    const result = await request.query(`
      SET NOCOUNT ON;
      
      IF OBJECT_ID('tempdb..#TempPivot') IS NOT NULL DROP TABLE #TempPivot;
      IF OBJECT_ID('tempdb..#TempPivotVendedor') IS NOT NULL DROP TABLE #TempPivotVendedor;
      IF OBJECT_ID('tempdb..#TempPivotTotalDias') IS NOT NULL DROP TABLE #TempPivotTotalDias;
                                                                                                        
      SELECT                                                                                                       
        CASE WHEN RIGHT(CONVERT(VARCHAR(10), PED_DTP, 105), 7)= RIGHT(CONVERT(VARCHAR(10), GetDate(), 105), 7) THEN 
            1                                                                                                   
            ELSE                                                                                                
            0                                                                                                   
        END as MESANOATUAL                                                                                        
        ,CASE WHEN isnull(CASE WHEN a.PED_REV = 10 THEN 0 WHEN a.PED_REV = 4 THEN 0 WHEN a.PED_REV = 9 THEN 0 ELSE g.IPE_VTL END,0) > 0  THEN 
            1                                                                                                   
            ELSE                                                                                                
            0                                                                                                   
        END as POSSUIVALOR                                                                                        
        ,CASE WHEN a.PED_REV = 10 THEN 0 WHEN a.PED_REV = 4 THEN 0 WHEN a.PED_REV = 9 THEN 0 ELSE g.IPE_VTL END as IPE_VTL    
        ,g.IPE_TPV                                                                                                
        ,a.PED_COD                                                                                                
        ,a.PED_CDI                                                                                                
        ,CONVERT(date,a.PED_DTP) as [PED_DTP]                                                                     
        ,a.PED_REV                                                                                                
        ,a.CLI_COD                                                                                                
        ,a.FUN_COD
	,a.TPP_DES
        ,a.CLI_RAZ                                                                                                
        ,a.CLI_FAN
        ,b.CLI_DUP                                                                                        
        ,b.CLI_CID                                                                                                
        ,b.CLI_UF                                                                                                 
        ,a.TRP_COD                                                                                                
        ,CASE WHEN a.PED_REV = 4 THEN 'Introdução Consignado'                                                     
              WHEN a.PED_REV = 1 THEN 'Venda Avulsa'                                                                
              WHEN a.PED_REV = 2 THEN 'Transferência'                                                               
              WHEN a.PED_REV = 3 THEN 'Borra'                                                                       
              WHEN a.PED_REV = 5 THEN 'Orçamento'                                                                   
              WHEN a.PED_REV = 6 THEN 'Nota Fiscal Serviço'                                                         
              WHEN a.PED_REV = 7 THEN 'Reposição'                                                                   
              WHEN a.PED_REV = 8 THEN 'Indrodução Venda'                                                           
              WHEN a.PED_REV = 9 THEN 'Complemento Consignação'                                                     
              WHEN a.PED_REV = 10 THEN 'Indrodução Bonificada'                                                     
        END as PED_REV_DES                                                                                        
        , CASE WHEN a.PED_STA = 'PND' THEN 'Pendente'                                                             
               WHEN a.PED_STA = 'APR' THEN 'Aprovado'                                                               
               WHEN a.PED_STA = 'CNC' THEN 'Cancelado'                                                             
               WHEN a.PED_STA = 'PRO' THEN 'Em Produção'                                                           
               WHEN a.PED_STA = 'FAT' THEN 'Faturado'                                                               
               WHEN a.PED_STA = 'ESP' THEN 'Em Separação'                                                          
               WHEN a.PED_STA = 'SPC' THEN 'Separação Concluída'                                                    
               WHEN a.PED_STA = 'FTP' THEN 'Faturado Parcial'                                                       
            END as PED_STA_DES                                                                                    
        ,c.FUN_NOM                                                                                                
        ,d.TRP_RAZ                                                                                                
        ,i.GRP_DES                                                                                                
        ,j.UNI_DES                                                                                                
        into #TempPivot                                                                                           
        FROM cad_ped a WITH (NOLOCK)                                                                              
             JOIN cad_cli b WITH (NOLOCK) on b.CLI_COD=a.CLI_COD                                                  
        left JOIN cad_fun c WITH (NOLOCK) on c.FUN_COD=ISNULL(a.FUN_COD,b.FUN_COD)                                
        left JOIN cad_trp d WITH (NOLOCK) on d.TRP_COD=a.TRP_COD                                                  
             JOIN cad_ipe g WITH (NOLOCK) on g.PED_COD = a.PED_COD                                                
             JOIN cad_prc h WITH (NOLOCK) on h.PRC_COD = g.PRC_COD                                                
             JOIN cad_grp i WITH (NOLOCK) on i.GRP_COD = h.GRP_COD                                                
             JOIN cad_uni j WITH (NOLOCK) on j.UNI_COD = g.UNI_COD                                                
       WHERE  a.PED_STA not in ('CNC','PRO')  
         AND  CONVERT(varchar,a.PED_DTP,112) >= '20250601'
--         AND  CONVERT(varchar,a.PED_DTP,112) >= CONVERT(varchar,DATEADD(day, -30, GETDATE()),112); -- Só últimos 30 dias
                                                                                                        
       DECLARE @TOTALDIAS int;                                                                                        
       SET @TOTALDIAS = (select count(distinct PED_DTP) from #TempPivot where RIGHT(CONVERT(VARCHAR(10), PED_DTP, 105), 7) = RIGHT(CONVERT(VARCHAR(10), GetDate(), 105), 7));
                                                                                                        
        ;With tabela3 AS                                                                                            
        (                                                                                                   
            select CASE WHEN sum(POSSUIVALOR) > 0 THEN 1                                                          
                                                  ELSE 0                                                          
                                      END as POSSUIVALOR                                                          
                   ,FUN_NOM                                                                                         
                   ,FUN_COD                                                                                         
                   ,sum(IPE_VTL) as IPE_VTL                                                                         
                   ,PED_DTP                                                                                         
              from #TempPivot                                                                                       
             where MESANOATUAL = 1                                                                                  
             group by PED_DTP,FUN_COD,FUN_NOM                                                                       
        )                                                                                                   
        select sum(POSSUIVALOR) as QTDE_DIAS_VENDA,FUN_COD,FUN_NOM,@TOTALDIAS as TOTAL_DIAS_VENDA                    
            into #TempPivotVendedor                                                                                
            from tabela3                                                                                            
        group by FUN_COD,FUN_NOM;                                                                                    
                                                                                                        
        select CASE WHEN d.FUN_DTD IS NOT NULL THEN 'INATIVO' ELSE ISNULL(e.FUN_NOM,'NAO SUPERVISIONADO') END as [SUP]
                  ,a.FUN_NOM as NOME                                                                                  
                  ,(RIGHT(CONVERT(VARCHAR(10), PED_DTP, 105), 7))  as MESANO                                       
                ,a.MESANOATUAL
                ,a.POSSUIVALOR
                ,a.IPE_VTL
                ,a.PED_COD
                ,a.PED_DTP
		,a.CLI_FAN
                ,a.CLI_DUP
		,a.CLI_CID
		,a.CLI_UF
		,a.TPP_DES
                ,a.PED_REV_DES
                ,a.PED_STA_DES
                ,a.FUN_NOM
                ,a.GRP_DES
                ,b.QTDE_DIAS_VENDA
                ,b.TOTAL_DIAS_VENDA
        from #TempPivot a                                                                                          
        LEFT JOIN #TempPivotVendedor b ON a.FUN_COD = b.FUN_COD                                                     
        LEFT JOIN cad_fun d WITH (NOLOCK) ON d.FUN_COD = a.FUN_COD                                                                
        LEFT JOIN cad_fun e WITH (NOLOCK) ON e.FUN_COD = d.FUN_CFS;                                                                

        DROP TABLE #TempPivot;
        DROP TABLE #TempPivotVendedor;
    `);

    const endTime = Date.now();
    const executionTime = endTime - startTime;
    console.log(`✅ Query executada com sucesso em ${executionTime}ms`);
    console.log(`📊 Retornou ${result.recordset.length} registros`);

    res.json({ 
      recordset: result.recordset,
      executionTime: executionTime,
      recordCount: result.recordset.length 
    });
  } catch (err) {
    console.error('❌ Erro ao executar a query no banco de dados:', err);
    res.status(500).json({ 
      error: 'Falha ao executar a query no banco de dados.',
      details: err.message 
    });
  }
});

// NOVO ENDPOINT PARA LTV DATA
app.get('/ltv-data', async (req, res) => {
  try {
    const currentPool = await getPool();
    if (!currentPool || !currentPool.connected) {
      return res.status(503).json({ error: 'Serviço de banco de dados indisponível.' });
    }

    const request = currentPool.request();
    request.timeout = 120000; // 2 minutos de timeout

    console.log('🔄 Iniciando execução da query de LTV...');
    const startTime = Date.now();

    const ltvQuery = `
      SET NOCOUNT ON;
      IF OBJECT_ID('tempdb..#IPE') IS NOT NULL DROP TABLE #IPE;
      IF OBJECT_ID('tempdb..#Dias') IS NOT NULL DROP TABLE #Dias;

      -- Temporária para itens de pedido
      Select 
          Cast(SUM(PRO_QTD) as int) as Totaltens,
          Cast(SUM(Case When cad_ipe.ipe_tpv = 1 Then PRO_QTD else Null End) as int) as TotalItensVenda,
          Cast(SUM(Case When cad_ipe.ipe_tpv = 2 Then PRO_QTD else Null End) as int) as TotalItensBoni,
          Cast(SUM(Case When cad_ipe.ipe_tpv = 3 Then PRO_QTD else Null End) as int) as TotalItensTroca,
          SUM(Case when cad_ped.ped_rev = 4 Then cad_ipe.ipe_vtl else null end) as ValorTotalItens,
          cad_cli.CLI_COD
          into #IPE
          from cad_ipe 
          Join cad_ped on cad_ipe.ped_cod = cad_ped.ped_cod
          Join cad_cli on cad_cli.cli_cod = cad_ped.cli_cod
          Where cad_ped.PED_STA = 'FAT'
          group by cad_cli.cli_cod
          -- ORDER BY cad_cli.CLI_COD -- ORDER BY não permitido em SELECT INTO para temp tables

      -- Temporária para calcular meses do cliente
      Select Distinct cli_cod, DATEDIFF(MONTH, Min(ped_Dtp), GETDATE()) AS Dias -- Renomeado para MesesAtivos
      into #Dias
      from cad_ped where ped_rev = 4
      group by cli_cod;
      
      -- Consulta principal para o LTV
      Select 
          TEMP.CLI_RAZ,
          TEMP.CLI_COD,
          TEMP.CLI_KIN,
          TEMP.CLI_DUP,
          TEMP.TotalConsig,
          TEMP.TotalRepo,
          TEMP.Diff,
          TEMP.TicketMedioRepo,
          TEMP.NumConsig,
          TEMP.NumRepo,
          TEMP.TotalItensVenda,
          TEMP.TotalItensBoni,
          TEMP.TotalItensTroca,
          TEMP.MesesConsig,
          TEMP.FUN_NOM,
          TEMP.CLI_STA_DES,
          TEMP.Intervalo
          From
      (
      Select 
          cad_cli.CLI_RAZ,
          cad_cli.CLI_COD,
          cad_cli.CLI_KIN,
          cad_cli.CLI_DUP,
          Case When Sum (Case When ped_rev = 4 Then Ped_Vlq Else 0 End) > 0 Then  
              Sum (Case When ped_rev = 4 Then Ped_Vlq Else 0 End) 
          Else
              #IPE.ValorTotalItens
          End
              as TotalConsig,
          Sum (Case 
              When ped_rev = 7 Then Ped_Vlq
              Else 0
              End) as TotalRepo,
          (Sum (Case When ped_rev = 4 Then Ped_Vlq Else 0 End) - Sum (Case When ped_rev = 7 Then Ped_Vlq Else 0 End)) as Diff, -- Adicionado Diff aqui
          Count(case When ped_rev = 4 then 1 else null End) as NumConsig,
          Count(case When ped_rev = 7 then 1 else null End) as NumRepo,
          #IPE.TotalItensVenda,
          #IPE.TotalItensBoni,
          #IPE.TotalItensTroca,
          MAX(#Dias.Dias) as MesesConsig,
          cad_fun.FUN_NOM,
          CASE cad_cli.CLI_STA WHEN 1 THEN 'Prospect' WHEN 2 THEN 'Ativo' WHEN 3 THEN 'Inativo' WHEN 4 THEN 'Bloqueado' END as [CLI_STA_DES], 
          Iif (DATEDIFF(day,cad_cli.CLI_DUP, GETDATE())  <= 60, 1,iif( DATEDIFF(day,cad_cli.CLI_DUP,  GETDATE()) >= 61 And DATEDIFF(day,cad_cli.CLI_DUP, GETDATE()) <= 90 , 2, 3)) as [Intervalo],
          CAST(
            (Case When Sum (Case When ped_rev = 7 Then Ped_Vlq Else 0 End) > 0 Then Sum (Case When ped_rev = 7 Then Ped_Vlq Else 0 End) Else 0 End) /
            (Case When Count(case When ped_rev = 7 then 1 else null End) > 0 Then Count(case When ped_rev = 7 then 1 else null End) Else 1 End) -- Evita divisão por zero
            as Decimal(18,2)
          ) as TicketMedioRepo -- Calculado aqui
          From cad_ped
          JOIN cad_cli on cad_cli.CLI_COD = cad_ped.CLI_COD
          JOIN CAD_FUN on cad_fun.FUN_COD = cad_cli.FUN_COD
          JOIN #Dias on #Dias.CLI_COD = cad_Ped.CLI_COD
          Join #IPE on #IPE.cli_cod = cad_ped.cli_cod
          Where cad_ped.Ped_Sta = 'FAT' 
          Group By cad_cli.CLI_COD, cad_cli.CLI_RAZ, #IPE.TotalItensVenda, #IPE.TotalItensBoni, #IPE.TotalItensTroca,
          cad_cli.CLI_KIN, #IPE.ValorTotalItens, cad_cli.cli_sta, cad_fun.fun_nom, cad_cli.CLI_DUP
          Having (
            Sum (Case 
            When ped_rev = 4 Then Ped_Vlq
            Else 0 
            End
            ) >= 0
          )
      ) as [TEMP]
      order by TEMP.CLI_COD;

      Drop table #IPE;
      Drop Table #Dias;
    `;

    const result = await request.query(ltvQuery);

    const endTime = Date.now();
    const executionTime = endTime - startTime;
    console.log(`✅ Query LTV executada com sucesso em ${executionTime}ms`);
    console.log(`📊 Retornou ${result.recordset.length} registros para LTV`);

    res.json({ 
      recordset: result.recordset,
      executionTime: executionTime,
      recordCount: result.recordset.length 
    });
  } catch (err) {
    console.error('❌ Erro ao executar a query de LTV no banco de dados:', err);
    res.status(500).json({ 
      error: 'Falha ao executar a query de LTV no banco de dados.',
      details: err.message 
    });
  }
});


// Inicializa a conexão com retry e depois inicia o servidor
connectWithRetry().then(() => {
  app.listen(PORT, HOST, () => {
    console.log(`🚀 API LEME-ME rodando em http://${HOST}:${PORT}`);
  });
});