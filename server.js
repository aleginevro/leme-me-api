// server.js — LEME-ME API (VERSÃO CORRIGIDA E OTIMIZADA)
const express = require('express');
const sql = require('mssql');
const cors = require('cors');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;
const HOST = '0.0.0.0';

app.use(cors());
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
  requestTimeout: 180000 // 3 minutos para queries longas
};

let pool = null;

async function getPool() {
  if (pool && pool.connected) return pool;
  try {
    console.log('🔄 Tentando (re)conectar ao DB...');
    pool = await sql.connect(dbConfig);
    console.log('✅ Conexão com DB estabelecida.');
    return pool;
  } catch (err) {
    console.error('❌ Falha ao obter pool de conexão:', err.message);
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
    request.timeout = 180000; // 3 minutos de timeout

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
        END as MESANOATUAL,                                                                                        
        CASE WHEN isnull(CASE WHEN a.PED_REV = 10 THEN 0 WHEN a.PED_REV = 4 THEN 0 WHEN a.PED_REV = 9 THEN 0 ELSE g.IPE_VTL END,0) > 0  THEN 
            1                                                                                                   
            ELSE                                                                                                
            0                                                                                                   
        END as POSSUIVALOR,                                                                                        
        CASE WHEN a.PED_REV = 10 THEN 0 WHEN a.PED_REV = 4 THEN 0 WHEN a.PED_REV = 9 THEN 0 ELSE g.IPE_VTL END as IPE_VTL,
        h.TPP_DES,    
        g.IPE_TPV,                                                                                                
        a.PED_COD,                                                                                                
        a.PED_CDI,                                                                                                
        CONVERT(date,a.PED_DTP) as [PED_DTP],                                                                     
        a.PED_REV,
        f.PED_REV_DES,
        a.PED_STA,
        e.PED_STA_DES,                                                                                            
        c.FUN_COD,
        c.FUN_NOM,
        b.CLI_RAZ,
        b.CLI_FAN,
        i.GRP_DES
      INTO #TempPivot
      from cad_ped a WITH (NOLOCK)
      LEFT JOIN cad_cli b WITH (NOLOCK) ON a.CLI_COD = b.CLI_COD
      LEFT JOIN cad_fun c WITH (NOLOCK) ON a.FUN_COD = c.FUN_COD
      LEFT JOIN sta_ped e WITH (NOLOCK) ON a.PED_STA = e.PED_STA_COD
      LEFT JOIN rev_ped f WITH (NOLOCK) ON a.PED_REV = f.PED_REV_COD
      LEFT JOIN cad_ipe g WITH (NOLOCK) ON a.PED_COD = g.PED_COD AND g.EMP_COD = a.EMP_COD
      LEFT JOIN cad_tpp h WITH (NOLOCK) ON g.TPP_COD = h.TPP_COD
      LEFT JOIN cad_grp i WITH (NOLOCK) ON g.GRP_COD = i.GRP_COD
      WHERE c.FUN_COD NOT IN (6,15,31,43,45,50,56)
      AND c.FUN_SIT = 'A'
      AND c.FUN_TFC IN (1,2)
      AND a.PED_REV <> 10
      AND CONVERT(varchar,a.PED_DTP,112) >= '20250101'
      AND CONVERT(varchar,a.PED_DTP,112) >= CONVERT(varchar,DATEADD(day, -30, GETDATE()),112);
      
      SELECT FUN_COD, COUNT(DISTINCT PED_DTP) AS QTDE_DIAS_VENDA, SUM(IPE_VTL) AS TOTAL_DIAS_VENDA
      INTO #TempPivotVendedor
      FROM #TempPivot
      WHERE POSSUIVALOR = 1
      GROUP BY FUN_COD;
      
      select 
        CASE WHEN d.FUN_DTD IS NOT NULL THEN 'INATIVO' ELSE ISNULL(e.FUN_NOM,'NAO SUPERVISIONADO') END as [SUP],
        a.FUN_NOM as NOME,                                                                                  
        (RIGHT(CONVERT(VARCHAR(10), PED_DTP, 105), 7)) as MESANO,                                       
        a.MESANOATUAL,
        a.POSSUIVALOR,
        a.IPE_VTL,
        a.PED_COD,
        a.PED_DTP,
        a.PED_REV_DES,
        a.PED_STA_DES,
        a.FUN_NOM,
        a.GRP_DES,
        a.TPP_DES, -- Adicionado para retornar ao frontend
        b.QTDE_DIAS_VENDA,
        b.TOTAL_DIAS_VENDA
      from #TempPivot a                                                                                          
      LEFT JOIN #TempPivotVendedor b ON a.FUN_COD = b.FUN_COD                                                     
      LEFT JOIN cad_fun d WITH (NOLOCK) ON d.FUN_COD = a.FUN_COD                                                                
      LEFT JOIN cad_fun e WITH (NOLOCK) ON e.FUN_COD = d.FUN_CFS;

      DROP TABLE #TempPivot;
      DROP TABLE #TempPivotVendedor;
    `);

    const endTime = Date.now();
    const executionTime = endTime - startTime;
    console.log(`✅ Query executada em ${executionTime} ms. Retornando ${result.recordset.length} registros.`);

    res.json({ recordset: result.recordset, executionTime });

  } catch (err) {
    console.error('❌ Erro Crítico no Endpoint /dashboard-data:', err);
    res.status(500).json({ error: 'Erro interno do servidor ao processar a solicitação.', details: err.message });
  }
});

getPool().then(() => {
  app.listen(PORT, HOST, () => {
    console.log(`🚀 API LEME-ME rodando em http://${HOST}:${PORT}`);
  });
});