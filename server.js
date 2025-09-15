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

// Inicializa a conexão com retry e depois inicia o servidor
connectWithRetry().then(() => {
  app.listen(PORT, HOST, () => {
    console.log(`🚀 API LEME-ME rodando em http://${HOST}:${PORT}`);
  });
});