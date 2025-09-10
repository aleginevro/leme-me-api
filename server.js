// server.js — LEME-ME API (VERSÃO OTIMIZADA PARA PERFORMANCE)
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
       ,a.EMP_COD                                                                                                        									   
       ,a.ORC_COD                                                                                                        									   
       ,a.CDP_COD                                                                                                        									   
       ,a.FCS_COD                                                                                                        									   
       ,a.FCV_COD                                                                                                        									   
       ,a.PED_TIP                                                                                                        									   
             ,a.TBP_COD                                                                                                        									   
             ,a.PED_DTA                                                                                                        									   
             ,a.PED_DTE                                                                                                        									   
             ,CONVERT(date,a.PED_DTP) as [PED_DTP]                                                                                                         									   
             ,a.PED_RAS                                                                                                        									   
             ,a.PED_REV                                                                                                        									   
             ,a.PED_IEV_RAS                                                                                                    									   
             ,a.PED_IEV_CNAB                                                                                                   									   
             ,a.PED_PRE                                                                                                        									   
             ,a.PED_DRP                                                                                                        									   
             ,a.PED_DER                                                                                                        									   
             ,a.PED_DTS                                                                                                        									   
             ,a.CLI_COD                                                                                                        									   
             ,a.CLI_CDS                                                                                                        									   
             ,a.CLI_CDC                                                                                                        									   
             ,a.VEI_COD                                                                                                        									   
             ,a.PPD_PPD                                                                                                        									   
             ,a.CLI_PDC                                                                                                        									   
             ,a.EDI_COD                                                                                                        									   
             ,a.FUN_COD                                                                                                        									   
             ,a.CLI_RAZ                                                                                                        									   
             ,a.CLI_FAN                                                                                                        									   
             ,a.CLI_TEL                                                                                                        									   
             ,b.CLI_CID                                                                                                        									   
             ,b.CLI_UF                                                                                                         									   
              ,a.REV_COD                                                                                                        									   
             ,a.TRP_COD                                                                                                        									   
             ,a.ID                                                                                                             									   
             ,a.PED_CES                                                                                                        									   
             ,a.PED_MDA                                                                                                        									   
             ,a.PED_MTC                                                                                                        									   
             ,CASE WHEN a.PED_NF = 0 THEN 'Sem' WHEN a.PED_NF = 1 THEN 'Com' END as PED_NF                                                                            
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
             ,CASE WHEN a.PED_MDE = 1 THEN 'N/A'                                                                         											   
             	  WHEN a.PED_MDE = 2 THEN 'Sedex'                                                                          											   
             	  WHEN a.PED_MDE = 3 THEN 'Pac'                                                                          											   
             	  WHEN a.PED_MDE = 4 THEN 'Expressa'                                                                          										   
             	  WHEN a.PED_MDE = 5 THEN 'Expedição'                                                                          										   
             	  WHEN a.PED_MDE = 6 THEN 'Presencial'                                                                          									   
             END as PED_MDE_DES                                                                                                    								   
             , CASE WHEN a.PED_STA = 'PND' THEN 'Pendente'                                                                     									   
             	   WHEN a.PED_STA = 'APR' THEN 'Aprovado'                                                                       									   
             	   WHEN a.PED_STA = 'CNC' THEN 'Cancelado'                                                                      									   
             	   WHEN a.PED_STA = 'PRO' THEN 'Em Produção'                                                                      									   
             	   WHEN a.PED_STA = 'FAT' THEN 'Faturado'                                                                       									   
             	   WHEN a.PED_STA = 'ESP' THEN 'Em Separação'                                                                   									   
             	   WHEN a.PED_STA = 'SPC' THEN 'Separação Concluída'                                                            									   
             	   WHEN a.PED_STA = 'FTP' THEN 'Faturado Parcial'                                                               									   
             	END as PED_STA_DES                                                                                                  								   
             ,CASE WHEN g.IPE_TPV = 1 THEN 'Venda'                                                                             									   
             	  WHEN g.IPE_TPV = 2 THEN 'Bonificação'                                                                         									   
             	  WHEN g.IPE_TPV = 3 THEN 'Troca/Devolução'                                                                     									   
             	  WHEN g.IPE_TPV = 4 THEN 'Consignado'                                                                          									   
             END as IPE_TPV_DES                                                                                                    								   
             ,a.PED_LCS                                                                                                        									   
             ,a.PED_ORI                                                                                                        									   
             ,a.PED_QTI                                                                                                        									   
             ,a.PED_QTU                                                                                                        									   
             ,a.PED_VLT                                                                                                        									   
             ,a.PED_VTE                                                                                                        									   
             ,a.PED_VLC                                                                                                        									   
             ,a.PED_PDE                                                                                                        									   
             ,a.PED_VLD                                                                                                        									   
             ,a.PED_VLA                                                                                                        									   
             ,a.PED_VCR                                                                                                        									   
             ,a.PED_VLF                                                                                                        									   
             ,a.PED_VDF                                                                                                        									   
             ,a.PED_VPB                                                                                                        									   
             ,a.PED_ECB                                                                                                        									   
             ,a.PED_EET                                                                                                        									   
             ,a.PED_ECO                                                                                                        									   
             ,a.PED_IMP                                                                                                        									   
             ,a.PED_TPF                                                                                                        									   
             ,a.PED_DAP                                                                                                        									   
             ,a.PED_DTC                                                                                                        									   
             ,a.PED_DTF                                                                                                        									   
             ,a.PED_DUA                                                                                                        									   
             ,a.PED_DSP                                                                                                        									   
             ,a.TPP_DES                                                                                                        									   
             ,a.PED_OBS                                                                                                        									   
             ,a.PED_FNG                                                                                                        									   
             ,a.PED_VDE                                                                                                        									   
             ,a.PED_RTE                                                                                                        									   
             ,a.PED_MDE                                                                                                        									   
             ,a.USU_UUA                                                                                                        									   
             ,a.USU_LOG                                                                                                        									   
             ,a.USU_APR                                                                                                        									   
             ,a.USU_CNC                                                                                                        									   
             ,a.USU_USP                                                                                                        									   
             ,a.PED_URL                                                                                                        									   
             ,a.PED_IDP                                                                                                        									   
             ,e.EMP_NMR                                                                                                        									   
             ,c.FUN_NOM                                                                                                        									   
             ,d.TRP_RAZ                                                                                                        									   
             ,b.CLI_STA                                                                                                        									   
             ,b.CLI_VLP                                                                                                                                               
             ,i.GRP_DES                                                                                                        									   
             ,j.UNI_DES                                                                                                        									   
             ,CASE b.CLI_TPP WHEN 'F' THEN dbo.formatarCPF(b.CLI_DOC) ELSE dbo.formatarCNPJ(b.CLI_DOC) END as [DOC]            									   
             ,dbo.returnRotas(b.CLI_COD) as [ROTAS]                                                                            									   
             ,b.CLI_KIN                                                                                                        									   
             ,b.CLI_QIK                                                                                                        									   
             ,CASE WHEN b.CLI_TIK = 99 THEN 'N/A'                                                                                 									   
             	  WHEN b.CLI_TIK = 4 THEN 'Introdução Consignado'                                                               									   
             	  WHEN b.CLI_TIK = 8 THEN 'Introdução Venda'                                                                    									   
             END as CLI_TIK_DES    																																   
             into #TempPivot																																		   
             FROM cad_ped a WITH (NOLOCK)                                                                                                   						   
                  JOIN cad_cli b WITH (NOLOCK) on b.CLI_COD=a.CLI_COD                                                                      						   
             left JOIN cad_fun c WITH (NOLOCK) on c.FUN_COD=ISNULL(a.FUN_COD,b.FUN_COD)                                                    						   
             left JOIN cad_trp d WITH (NOLOCK) on d.TRP_COD=a.TRP_COD                                                                      						   
             left JOIN cad_emp e WITH (NOLOCK) on e.EMP_COD=a.EMP_COD                                                                      						   
                  JOIN cad_ipe g WITH (NOLOCK) on g.PED_COD = a.PED_COD                                                                      						   
                  JOIN cad_prc h WITH (NOLOCK) on h.PRC_COD = g.PRC_COD                                                                      						   
                  JOIN cad_grp i WITH (NOLOCK) on i.GRP_COD = h.GRP_COD                                                                      						   
                  JOIN cad_uni j WITH (NOLOCK) on j.UNI_COD = g.UNI_COD                                                                      						   
            WHERE  a.PED_STA not in ('CNC','PRO')  and  CONVERT(varchar,a.PED_DTP,112) >= '20250101' 
            																																						   
            DECLARE @TOTALDIAS int																																	   
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
            	group by FUN_COD,FUN_NOM																															   
            																																						   
            	select CASE WHEN d.FUN_DTD IS NOT NULL THEN 'INATIVO' ELSE ISNULL(e.FUN_NOM,'NAO SUPERVISIONADO') END as [SUP]
                       ,isnull((convert(varchar(500),b.FUN_NOM) + ' - ' + convert(varchar(10),b.QTDE_DIAS_VENDA) + '/' + convert(varchar(10),b.TOTAL_DIAS_VENDA)),a.FUN_NOM) as FUN_NOM2  
            			,a.FUN_NOM as NOME																															  
                       ,(RIGHT(CONVERT(VARCHAR(10), PED_DTP, 105), 7))  as MESANO                                                                                    
            			,a.* 																																			  
            			,b.* 																																			  
            	from #TempPivot a																																	  
            	LEFT JOIN #TempPivotVendedor b ON a.FUN_COD = b.FUN_COD																							  
            	LEFT JOIN cad_fun d ON d.FUN_COD = a.FUN_COD																							  
            	LEFT JOIN cad_fun e ON e.FUN_COD = d.FUN_CFS																							  
                  																																																	 
                  union all																																															 
                  																																																	 
                  	select CASE WHEN a.FUN_DTD IS NOT NULL THEN 'INATIVO' ELSE ISNULL(ISNULL(b.FUN_APL,b.FUN_NOM),'NAO SUPERVISIONADO') END as [SUP], a.FUN_NOM as FUN_NOM2, a.FUN_NOM, RIGHT(CONVERT(VARCHAR(10), GetDate(), 105), 7) as MESANO ,1 as MESANOATUAL,0 as POSSUIVALOR,0 as IPE_VTL												 
                  	,null as IPE_TPV,null as PED_COD,null as PED_CDI,null as EMP_COD,null as ORC_COD,null as CDP_COD,null as FCS_COD,null as FCV_COD,null as PED_TIP,null as TBP_COD,null as PED_DTA				 
                  	,null as PED_DTE,null as PED_DTP,null as PED_RAS,null as PED_REV,null as PED_IEV_RAS,null as PED_IEV_CNAB,null as PED_PRE,null as PED_DRP,null as PED_DER,null as PED_DTS,null as CLI_COD		 
                  	,null as CLI_CDS,null as CLI_CDC,null as VEI_COD,null as PPD_PPD,null as CLI_PDC,null as EDI_COD,null as FUN_COD,null as CLI_RAZ,null as CLI_FAN,null as CLI_TEL,null as CLI_CID,null as CLI_UF	 
                  	,null as REV_COD,null as TRP_COD,null as ID,null as PED_CES,null as PED_MDA,null as PED_MTC,null as PED_NF,null as PED_REV_DES,null as PED_MDE_DES,null as PED_STA_DES,null as IPE_TPV_DES		 
                  	,null as PED_LCS,null as PED_ORI,null as PED_QTI,null as PED_QTU,null as PED_VLT,null as PED_VTE,null as PED_VLC,null as PED_PDE,null as PED_VLD,null as PED_VLA,null as PED_VCR,null as PED_VLF 
                  	,null as PED_VDF,null as PED_VPB,null as PED_ECB,null as PED_EET,null as PED_ECO,null as PED_IMP,null as PED_TPF,null as PED_DAP,null as PED_DTC,null as PED_DTF,null as PED_DUA,null as PED_DSP
                  	,null as TPP_DES,null as PED_OBS,null as PED_FNG,null as PED_VDE,null as PED_RTE,null as PED_MDE,null as USU_UUA,null as USU_LOG,null as USU_APR,null as USU_CNC,null as USU_USP,null as PED_URL
                  	,null as PED_IDP,null as EMP_NMR,null as FUN_NOM,null as TRP_RAZ,null as CLI_STA,null as CLI_VLP,null as GRP_DES,null as UNI_DES,null as DOC,null as ROTAS,null as CLI_KIN,null as CLI_QIK		
                  	,null as CLI_TIK_DES,null as QTDE_DIAS_VENDA,null as FUN_COD,null as FUN_NOM,null as TOTAL_DIAS_VENDA 
                   from cad_fun a left JOIN cad_fun b on b.FUN_COD = a.FUN_CFS where a.CAR_COD in (4,5,8,11) and a.FUN_COD not in (select b.FUN_COD from #TempPivot b GROUP by b.FUN_COD)
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