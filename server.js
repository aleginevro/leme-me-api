// server.js — LEME-ME API (VERSÃO COM A SUA QUERY EXATA E CORRIGIDA DO SSMS)
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
      IF OBJECT_ID('tempdb..#TempPivot') IS NOT NULL DROP TABLE #TempPivot;
      IF OBJECT_ID('tempdb..#TempPivotVendedor') IS NOT NULL DROP TABLE #TempPivotVendedor;

      -- Criação da tabela temporária #TempPivot
      SELECT
          a.PED_COD,
          a.PED_TIP,
          CONVERT(DATE, a.PED_DTP) AS PED_DTP,
          a.PED_REV,
          a.CLI_COD,
          a.FUN_COD,
          a.CLI_FAN,
          b.CLI_CID,
          b.CLI_UF,
          CASE WHEN a.PED_NF = 0 THEN 'Sem' ELSE 'Com' END AS PED_NF,
          CASE 
              WHEN a.PED_REV = 4 THEN 'Introdução Consignado'
              WHEN a.PED_REV = 1 THEN 'Venda Avulsa'
              WHEN a.PED_REV = 2 THEN 'Transferência'
              WHEN a.PED_REV = 3 THEN 'Borra'
              WHEN a.PED_REV = 5 THEN 'Orçamento'
              WHEN a.PED_REV = 6 THEN 'Nota Fiscal Serviço'
              WHEN a.PED_REV = 7 THEN 'Reposição'
              WHEN a.PED_REV = 8 THEN 'Introdução Venda'
              WHEN a.PED_REV = 9 THEN 'Complemento Consignação'
              WHEN a.PED_REV = 10 THEN 'Introdução Bonificada'
          END AS PED_REV_DES,
          CASE 
              WHEN a.PED_MDE = 1 THEN 'N/A'
              WHEN a.PED_MDE = 2 THEN 'Sedex'
              WHEN a.PED_MDE = 3 THEN 'Pac'
              WHEN a.PED_MDE = 4 THEN 'Expressa'
              WHEN a.PED_MDE = 5 THEN 'Expedição'
              WHEN a.PED_MDE = 6 THEN 'Presencial'
          END AS PED_MDE_DES,
          CASE 
              WHEN a.PED_STA = 'PND' THEN 'Pendente'
              WHEN a.PED_STA = 'APR' THEN 'Aprovado'
              WHEN a.PED_STA = 'CNC' THEN 'Cancelado'
              WHEN a.PED_STA = 'PRO' THEN 'Em Produção'
              WHEN a.PED_STA = 'FAT' THEN 'Faturado'
              WHEN a.PED_STA = 'ESP' THEN 'Em Separação'
              WHEN a.PED_STA = 'SPC' THEN 'Separação Concluída'
              WHEN a.PED_STA = 'FTP' THEN 'Faturado Parcial'
          END AS PED_STA_DES,
          CASE 
              WHEN g.IPE_TPV = 1 THEN 'Venda'
              WHEN g.IPE_TPV = 2 THEN 'Bonificação'
              WHEN g.IPE_TPV = 3 THEN 'Troca/Devolução'
              WHEN g.IPE_TPV = 4 THEN 'Consignado'
          END AS IPE_TPV_DES,
          a.PED_ORI,
          a.PED_QTI,
          a.PED_QTU,
          a.PED_VLT,
          a.TPP_DES, 
          e.EMP_NMR,
          c.FUN_NOM,
          b.CLI_STA,
          i.GRP_DES,
          CASE 
              WHEN b.CLI_TIK = 99 THEN 'N/A'
              WHEN b.CLI_TIK = 4 THEN 'Introdução Consignado'
              WHEN b.CLI_TIK = 8 THEN 'Introdução Venda'
          END AS CLI_TIK_DES
      INTO #TempPivot
      FROM cad_ped a WITH (NOLOCK)
      JOIN cad_cli b WITH (NOLOCK) ON b.CLI_COD = a.CLI_COD
      LEFT JOIN cad_fun c WITH (NOLOCK) ON c.FUN_COD = ISNULL(a.FUN_COD, b.FUN_COD)
      LEFT JOIN cad_trp d WITH (NOLOCK) ON d.TRP_COD = a.TRP_COD
      LEFT JOIN cad_emp e WITH (NOLOCK) ON e.EMP_COD = a.EMP_COD
      JOIN cad_ipe g WITH (NOLOCK) ON g.PED_COD = a.PED_COD
      JOIN cad_prc h WITH (NOLOCK) ON h.PRC_COD = g.PRC_COD
      JOIN cad_grp i WITH (NOLOCK) ON i.GRP_COD = h.GRP_COD
      JOIN cad_uni j WITH (NOLOCK) ON j.UNI_COD = g.UNI_COD
      WHERE a.PED_STA NOT IN ('CNC', 'PRO')  
        AND CONVERT(VARCHAR, a.PED_DTP, 112) >= '20250101';

      -- Criação da tabela temporária #TempPivotVendedor
      ;WITH tabela3 AS (
          SELECT
              FUN_NOM,
              FUN_COD,
              PED_DTP
          FROM #TempPivot
          GROUP BY PED_DTP, FUN_COD, FUN_NOM
      )
      SELECT
          FUN_COD,
          FUN_NOM
      INTO #TempPivotVendedor
      FROM tabela3
      GROUP BY FUN_COD, FUN_NOM;

      -- Consulta final
      SELECT
          CASE WHEN d.FUN_DTD IS NOT NULL THEN 'INATIVO' ELSE ISNULL(e.FUN_NOM, 'NAO SUPERVISIONADO') END AS [SUP],
          a.FUN_NOM AS NOME,
          a.*,
          b.*
      FROM #TempPivot a
      LEFT JOIN #TempPivotVendedor b ON a.FUN_COD = b.FUN_COD
      LEFT JOIN cad_fun d ON d.FUN_COD = a.FUN_COD
      LEFT JOIN cad_fun e ON e.FUN_COD = d.FUN_CFS;
    `;

    const result = await request.query(query);
    const endTime = Date.now();
    console.log(`✅ Query executada em ${endTime - startTime}ms.`);

    res.json({ recordset: result.recordset });

  } catch (err) {
    console.error('❌ Erro na API /dashboard-data:', err);
    console.error('Detalhes do erro:', err.originalError || err);
    res.status(500).json({ error: 'Erro interno do servidor ao buscar dados.', details: err.message });
  }
});

app.listen(PORT, HOST, () => {
  console.log(`🚀 Servidor LEME-ME API rodando na porta ${PORT}`);
  console.log(`📡 Endpoint principal: http://${HOST}:${PORT}/dashboard-data`);
});