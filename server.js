// server.js — LEME-ME API (VERSÃO FINAL COM TRATAMENTO DE ERRO)
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
  pool: { max: 10, min: 0, idleTimeoutMillis: 30000 }
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

// NOVO ENDPOINT PARA TESTAR QUAIS TABELAS EXISTEM
app.get('/test-tables', async (req, res) => {
  try {
    const currentPool = await getPool();
    if (!currentPool || !currentPool.connected) {
      return res.status(503).json({ error: 'Serviço de banco de dados indisponível.' });
    }

    const request = currentPool.request();
    const result = await request.query(`
      SELECT TABLE_SCHEMA, TABLE_NAME 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_TYPE = 'BASE TABLE'
      ORDER BY TABLE_SCHEMA, TABLE_NAME
    `);

    res.json({ tabelas: result.recordset });
  } catch (err) {
    console.error('Erro ao listar tabelas:', err);
    res.status(500).json({ 
      error: 'Falha ao listar tabelas do banco de dados.',
      details: err.message 
    });
  }
});

// NOVO ENDPOINT SIMPLIFICADO PARA TESTAR DADOS
app.get('/dashboard-data-simple', async (req, res) => {
  try {
    const currentPool = await getPool();
    if (!currentPool || !currentPool.connected) {
      return res.status(503).json({ error: 'Serviço de banco de dados indisponível.' });
    }

    const request = currentPool.request();
    // Query muito simples para começar
    const result = await request.query(`
      SELECT TOP 10 * 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_TYPE = 'BASE TABLE'
    `);

    res.json({ recordset: result.recordset, message: 'Teste básico funcionando' });
  } catch (err) {
    console.error('Erro na query simples:', err);
    res.status(500).json({ 
      error: 'Falha ao executar query simples.',
      details: err.message 
    });
  }
});


// ENDPOINT CORRIGIDO PARA DADOS DO DASHBOARD
app.get('/dashboard-data', async (req, res) => {
  try {
    const currentPool = await getPool();
    if (!currentPool || !currentPool.connected) {
      return res.status(503).json({ error: 'Serviço de banco de dados indisponível.' });
    }

    const request = currentPool.request();
    request.timeout = 60000; // 60 segundos de timeout

    const result = await request.query(`
      IF OBJECT_ID('tempdb..#TempPivot') IS NOT NULL DROP TABLE #TempPivot;
      IF OBJECT_ID('tempdb..#TempPivotVendedor') IS NOT NULL DROP TABLE #TempPivotVendedor;
      IF OBJECT_ID('tempdb..#TempPivotTotalDias') IS NOT NULL DROP TABLE #TempPivotTotalDias;

      SELECT
       CASE WHEN RIGHT(CONVERT(VARCHAR(10), PED_DTP, 105), 7)= RIGHT(CONVERT(VARCHAR(10), GetDate(), 105), 7) THEN 1 ELSE 0 END as MESANOATUAL,
       CASE WHEN isnull(CASE WHEN a.PED_REV IN (10, 4, 9) THEN 0 ELSE g.IPE_VTL END,0) > 0 THEN 1 ELSE 0 END as POSSUIVALOR,
       CASE WHEN a.PED_REV IN (10, 4, 9) THEN 0 ELSE g.IPE_VTL END as IPE_VTL,
       g.IPE_TPV, a.PED_COD, a.PED_CDI, a.EMP_COD, a.ORC_COD, a.CDP_COD, a.FCS_COD, a.FCV_COD, a.PED_TIP,
       a.TBP_COD, a.PED_DTA, a.PED_DTE, CONVERT(date,a.PED_DTP) as [PED_DTP], a.PED_RAS, a.PED_REV, a.PED_IEV_RAS,
       a.PED_IEV_CNAB, a.PED_PRE, a.PED_DRP, a.PED_DER, a.PED_DTS, a.CLI_COD, a.CLI_CDS, a.CLI_CDC, a.VEI_COD,
       a.PPD_PPD, a.CLI_PDC, a.EDI_COD, a.FUN_COD, a.CLI_RAZ, a.CLI_FAN, a.CLI_TEL, b.CLI_CID, b.CLI_UF,
       a.REV_COD, a.TRP_COD, a.ID, a.PED_CES, a.PED_MDA, a.PED_MTC,
       CASE WHEN a.PED_NF = 0 THEN 'Sem' WHEN a.PED_NF = 1 THEN 'Com' END as PED_NF,
       CASE a.PED_REV
          WHEN 4 THEN 'Introdução Consignado' WHEN 1 THEN 'Venda Avulsa' WHEN 2 THEN 'Transferência'
          WHEN 3 THEN 'Borra' WHEN 5 THEN 'Orçamento' WHEN 6 THEN 'Nota Fiscal Serviço'
          WHEN 7 THEN 'Reposição' WHEN 8 THEN 'Indrodução Venda' WHEN 9 THEN 'Complemento Consignação'
          WHEN 10 THEN 'Indrodução Bonificada'
       END as PED_REV_DES,
       CASE a.PED_MDE
          WHEN 1 THEN 'N/A' WHEN 2 THEN 'Sedex' WHEN 3 THEN 'Pac' WHEN 4 THEN 'Expressa'
          WHEN 5 THEN 'Expedição' WHEN 6 THEN 'Presencial'
       END as PED_MDE_DES,
       CASE a.PED_STA
           WHEN 'PND' THEN 'Pendente' WHEN 'APR' THEN 'Aprovado' WHEN 'CNC' THEN 'Cancelado'
           WHEN 'PRO' THEN 'Em Produção' WHEN 'FAT' THEN 'Faturado' WHEN 'ESP' THEN 'Em Separação'
           WHEN 'SPC' THEN 'Separação Concluída' WHEN 'FTP' THEN 'Faturado Parcial'
        END as PED_STA_DES,
       CASE g.IPE_TPV
          WHEN 1 THEN 'Venda' WHEN 2 THEN 'Bonificação' WHEN 3 THEN 'Troca/Devolução' WHEN 4 THEN 'Consignado'
       END as IPE_TPV_DES,
       a.PED_LCS, a.PED_ORI, a.PED_QTI, h.SUP, j.NOME, f.CDP_DES, d.TBP_DES, g.GRP_DES, e.PRO_COD, a.PED_VLT, a.PED_VLD,
       ISNULL(NULLIF(TTP.TTP_DES, ''), 'NÃO INFORMADO') AS TTP_DES, ISNULL(CLI.CLI_KIN, 'NÃO') AS CLI_KIN
      INTO #TempPivot
      FROM PED a
      LEFT JOIN CLI b ON a.CLI_COD = b.CLI_COD AND a.EMP_COD = b.EMP_COD
      LEFT JOIN CDP f ON f.CDP_COD = a.CDP_COD AND f.EMP_COD = a.EMP_COD
      LEFT JOIN TBP d ON d.TBP_COD = a.TBP_COD
      INNER JOIN IPE g ON g.PED_COD = a.PED_COD AND g.EMP_COD = a.EMP_COD
      LEFT JOIN PRO e ON e.PRO_COD = g.PRO_COD AND e.EMP_COD = g.EMP_COD
      LEFT JOIN VENDEDOR j ON j.CODIGO = a.FUN_COD
      LEFT JOIN SUPERVISOR h ON h.CODIGO = j.SUPERVISOR
      LEFT JOIN TTP TTP ON A.TTP_COD = TTP.TTP_COD
      LEFT JOIN CLI CLI ON A.CLI_COD = CLI.CLI_COD
      WHERE CONVERT(date,a.PED_DTP) >= DATEADD(day, -30, GETDATE())
      AND a.PED_REV NOT IN (2, 5, 3) AND a.PED_STA <> 'CNC';

      SELECT * FROM #TempPivot;
    `);

    res.json({ recordset: result.recordset });

  } catch (err) {
    console.error('SQL Error:', err);
    res.status(500).json({
      error: 'Falha ao executar a query no banco de dados.',
      details: err.message
    });
  }
});

app.listen(PORT, HOST, async () => {
  console.log(`🚀 API LEME-ME rodando em http://${HOST}:${PORT}`);
  await connectWithRetry();
});