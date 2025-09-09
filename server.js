// server.js — LEME-ME API
const express = require('express');
const sql = require('mssql');
const cors = require('cors');
require('dotenv').config(); // <-- Adicione esta linha para carregar variáveis de ambiente

const app = express();
const PORT = process.env.PORT || 3000;
const HOST = '0.0.0.0';

app.use(cors());
app.use(express.json());

// ---- CONFIG DB (host + porta fixa; sem instanceName) ----
const dbConfig = {
  server: process.env.DB_HOST || process.env.DB_SERVER || 'fenixsys.emartim.com.br',
  port: parseInt(process.env.DB_PORT || '20902', 10), // <- importante para seu ambiente
  database: process.env.DB_NAME || process.env.DB_DATABASE || 'LevemeFenix', // <-- ALTERADO AQUI PARA 'LevemeFenix'
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  options: {
    encrypt: (process.env.DB_ENCRYPT || 'false') === 'true',
    trustServerCertificate: (process.env.DB_TRUST_SERVER_CERTIFICATE || 'true') === 'true',
    enableArithAbort: true
  },
  pool: { max: 10, min: 0, idleTimeoutMillis: 30000 }
};

// Mantém um pool global (não feche em cada requisição)
let pool = null;

// Conectar com tentativas, sem derrubar o processo
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

// Garante um pool pronto (1 tentativa rápida on-demand)
async function getPool() {
  if (pool && pool.connected) return pool;
  try {
    pool = await sql.connect(dbConfig);
    return pool;
  } catch (err) { // Capturar o erro para logar se a conexão on-demand falhar
    console.error('Falha ao obter pool de conexão:', err.message);
    return null;
  }
}

// Exemplo de uma rota de API para testar a conexão
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

// Iniciar o servidor e tentar conectar ao DB
app.listen(PORT, HOST, async () => {
  console.log(`🚀 API LEME-ME rodando em http://${HOST}:${PORT}`);
  await connectWithRetry(); // Tenta conectar ao DB ao iniciar
});