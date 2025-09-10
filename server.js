// server.js — LEME-ME API (VERSÃO FINAL E ROBUSTA)
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
  try {
    if (pool && pool.connected) {
      return pool;
    }
    console.log('🔄 Tentando (re)conectar ao DB...');
    pool = await sql.connect(dbConfig);
    console.log('✅ Conexão com DB estabelecida.');
    
    pool.on('error', err => {
      console.error('❌ Erro no Pool de Conexão SQL:', err);
      pool = null; // Força a recriação da conexão na próxima chamada
    });

    return pool;
  } catch (err) {
    console.error('❌ Falha crítica ao obter pool de conexão:', err.message);
    pool = null; // Garante que não tentaremos usar um pool inválido
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
    res.status(503).json({ status: 'API funcionando, mas DB indisponível.' });
  }
});

// ENDPOINT OTIMIZADO PARA PERFORMANCE COM LOG DE ERRO DETALHADO
app.get('/dashboard-data', async (req, res) => {
  try {
    const currentPool = await getPool();
    if (!currentPool) {
      return res.status(503).json({ error: 'Serviço de banco de dados indisponível. Pool não foi criado.' });
    }

    const request = currentPool.request();
    request.timeout = 180000; // 3 minutos de timeout

    console.log('🔄 Iniciando execução da query...');
    const startTime = Date.now();

    const query = `
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
        g.IPE_TPV,
        a.PED_COD,
        a.PED_CDI,
        CONVERT(date,a.PED_DTP) as [PED_DTP],
        a.PED_REV,
        h.REV_DES as [PED_REV_DES],
        a.PED_STA,
        i.STA_DES as [PED_STA_DES],
        a.FUN_COD,
        d.FUN_NOM,
        g.GRP_COD,
        e.GRP_DES,
        j.TPP_DES -- Campo TPP_DES adicionado aqui
      INTO #TempPivot
      FROM mov_ped a  WITH (NOLOCK)
      INNER JOIN cad_cli b WITH (NOLOCK) ON a.CLI_COD = b.CLI_COD
      LEFT JOIN cad_fun d WITH (NOLOCK) ON a.FUN_COD = d.FUN_COD
      LEFT JOIN mov_ipe g WITH (NOLOCK) ON a.PED_COD = g.PED_COD
      LEFT JOIN cad_grp e WITH (NOLOCK) ON g.GRP_COD = e.GRP_COD
      LEFT JOIN cad_rev h WITH (NOLOCK) ON a.PED_REV = h.REV_COD
      LEFT JOIN cad_sta i WITH (NOLOCK) ON a.PED_STA = i.STA_COD
      LEFT JOIN cad_tpp j WITH (NOLOCK) ON a.TPP_COD = j.TPP_COD -- JOIN para buscar TPP_DES
      WHERE CONVERT(varchar,a.PED_DTP,112) >= CONVERT(varchar,DATEADD(day, -30, GETDATE()),112);

      SELECT FUN_COD, COUNT(DISTINCT PED_DTP) AS QTDE_DIAS_VENDA, DATEDIFF(day, MIN(PED_DTP), MAX(PED_DTP)) + 1 AS TOTAL_DIAS_VENDA
      INTO #TempPivotVendedor
      FROM #TempPivot
      WHERE POSSUIVALOR > 0
      GROUP BY FUN_COD;
      
      SELECT
        CASE WHEN d.FUN_DTD IS NOT NULL THEN 'INATIVO' ELSE ISNULL(e.FUN_NOM, 'NAO SUPERVISIONADO') END as [SUP],
        a.FUN_NOM as NOME,
        (RIGHT(CONVERT(VARCHAR(10), a.PED_DTP, 105), 7)) as MESANO,
        a.MESANOATUAL,
        a.POSSUIVALOR,
        a.IPE_VTL,
        a.PED_COD,
        a.PED_DTP,
        a.PED_REV_DES,
        a.PED_STA_DES,
        a.FUN_NOM as [FUN_NOM_DUPLICADO], -- Evita conflito de nome
        a.GRP_DES,
        a.TPP_DES, -- <<< GARANTIDO NO RESULTADO FINAL
        b.QTDE_DIAS_VENDA,
        b.TOTAL_DIAS_VENDA
      FROM #TempPivot a
      LEFT JOIN #TempPivotVendedor b ON a.FUN_COD = b.FUN_COD
      LEFT JOIN cad_fun d WITH (NOLOCK) ON d.FUN_COD = a.FUN_COD
      LEFT JOIN cad_fun e WITH (NOLOCK) ON e.FUN_COD = d.FUN_CFS;
    `;

    const result = await request.query(query);
    const endTime = Date.now();
    console.log(`✅ Query executada com sucesso em ${(endTime - startTime) / 1000}s. Registros: ${result.recordset.length}`);
    
    res.status(200).json(result);

  } catch (err) {
    // LOG DE ERRO DETALHADO!
    console.error('======================================================');
    console.error('💥 ERRO CRÍTICO AO EXECUTAR A QUERY /dashboard-data 💥');
    console.error('======================================================');
    console.error('Mensagem:', err.message);
    console.error('Código do Erro:', err.code);
    console.error('Stack Trace:', err.stack);
    console.error('------------------------------------------------------');
    
    res.status(500).json({ 
      error: 'Erro interno no servidor ao processar a solicitação.',
      details: err.message // Envia a mensagem de erro para o frontend
    });
  }
});

// INICIALIZAÇÃO DO SERVIDOR
getPool().then(() => {
  app.listen(PORT, HOST, () => {
    console.log(`🚀 API LEME-ME rodando em http://${HOST}:${PORT}`);
  });
}).catch(err => {
    console.error("Falha ao iniciar o servidor pois o DB não conectou inicialmente:", err);
});