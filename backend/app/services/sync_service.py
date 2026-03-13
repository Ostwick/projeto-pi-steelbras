from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import logging

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from app.config import settings

logger = logging.getLogger(__name__)

COST_MAP_SQL = """
DECLARE @ProdutoCodigo CHAR(60) = ?;
DECLARE @DataInicio DATE = ?;
DECLARE @DataFim DATE = ?;
DECLARE @FilialCodigo DECIMAL(14,0) = ?;

SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#BomExplodida') IS NOT NULL DROP TABLE #BomExplodida;

CREATE TABLE #BomExplodida (
    Id INT IDENTITY(1,1),
    Nivel INT,
    ProdutoPai CHAR(60),
    ProdutoFilho CHAR(60),
    DescricaoFilho VARCHAR(120),
    QuantidadeBase DECIMAL(18,6),
    TipoProduto VARCHAR(20),
    TemEstrutura CHAR(1),
    NumeroHierarquia VARCHAR(100)
);

;WITH BomRecursiva AS (
    SELECT 
        0 AS Nivel, CAST(NULL AS CHAR(60)) AS ProdutoPai,
        p.PRO_Codigo AS ProdutoFilho, p.PRO_Descricao AS DescricaoFilho,
        CAST(1.000000 AS DECIMAL(18,6)) AS QuantidadeBase,
        CASE WHEN p.PRO_Codigo LIKE 'SA%' THEN 'SA' WHEN p.PRO_Codigo LIKE 'SE%' OR p.PRO_Codigo LIKE 'MP%' THEN 'MP' ELSE 'PA' END AS TipoProduto,
        CASE WHEN EXISTS (SELECT 1 FROM ENG_ESTRUTURA e WHERE e.ENG_EstruturaProduto = p.PRO_Codigo AND e.ENG_EstruturaAtiva = 'S') THEN 'S' ELSE 'N' END AS TemEstrutura,
        CAST('000' AS VARCHAR(100)) AS NumeroHierarquia
    FROM PRO_PRODUTO p WHERE RTRIM(p.PRO_Codigo) = RTRIM(@ProdutoCodigo)
    UNION ALL
    SELECT 
        r.Nivel + 1, r.ProdutoFilho, ei.ENG_EstruturaItemProduto, p.PRO_Descricao,
        CAST(r.QuantidadeBase * ei.ENG_EstruturaItemQuantidade AS DECIMAL(18,6)),
        CASE WHEN ei.ENG_EstruturaItemProduto LIKE 'SA%' THEN 'SA' WHEN ei.ENG_EstruturaItemProduto LIKE 'SE%' OR ei.ENG_EstruturaItemProduto LIKE 'MP%' THEN 'MP' ELSE 'PA' END,
        CASE WHEN EXISTS (SELECT 1 FROM ENG_ESTRUTURA e2 WHERE e2.ENG_EstruturaProduto = ei.ENG_EstruturaItemProduto AND e2.ENG_EstruturaAtiva = 'S') THEN 'S' ELSE 'N' END,
        CAST(r.NumeroHierarquia + '.' + RIGHT('000' + CAST(ei.ENG_EstruturaItemSeq AS VARCHAR), 3) AS VARCHAR(100))
    FROM BomRecursiva r
    INNER JOIN ENG_ESTRUTURA e ON RTRIM(e.ENG_EstruturaProduto) = RTRIM(r.ProdutoFilho) AND e.ENG_EstruturaAtiva = 'S' AND e.ENG_EstruturaTipo = 'P'
    INNER JOIN ENG_ESTRUTURAITEM ei ON ei.ENG_EstruturaProduto = e.ENG_EstruturaProduto AND ei.ENG_EstruturaNumero = e.ENG_EstruturaNumero
    INNER JOIN PRO_PRODUTO p ON p.PRO_Codigo = ei.ENG_EstruturaItemProduto
    WHERE r.Nivel < 6
)
INSERT INTO #BomExplodida SELECT Nivel, ProdutoPai, ProdutoFilho, DescricaoFilho, QuantidadeBase, TipoProduto, TemEstrutura, NumeroHierarquia FROM BomRecursiva ORDER BY NumeroHierarquia;

SELECT 
    RTRIM(ProdutoFilho) AS produto_codigo,
    DescricaoFilho AS produto_descricao,
    TipoProduto AS tipo_produto,
    TemEstrutura AS tem_estrutura,
    NumeroHierarquia AS numero_hierarquia,
    Nivel AS nivel,
    CASE WHEN ProdutoPai IS NULL THEN NULL ELSE RTRIM(ProdutoPai) END AS pai_codigo,
    QuantidadeBase AS quantidade_base
FROM #BomExplodida
ORDER BY NumeroHierarquia;

SELECT 
    fo.EST_FechamentoOrdProduto AS produto_codigo,
    fo.EST_FechamentoOrdOrigemNumero AS op_numero,
    ISNULL(cc.CCT_Descricao, '-') AS centro_custo,
    CAST(fhr.EST_FechamentoOrdHrCentro AS VARCHAR) AS centro_codigo,
    fo.EST_FechamentoOrdQuantidade AS quantidade,
    SUM(fhr.EST_FechamentoOrdHrTempo) AS tempo_horas,
    CASE WHEN SUM(fhr.EST_FechamentoOrdHrTempo) > 0 THEN fo.EST_FechamentoOrdQuantidade / SUM(fhr.EST_FechamentoOrdHrTempo) ELSE 0 END AS qtd_por_hora,
    fo.EST_FechamentoOrdValorElaborac + fo.EST_FechamentoOrdValorDespDire + fo.EST_FechamentoOrdValorDespCent AS custo_mo,
    CASE WHEN SUM(fhr.EST_FechamentoOrdHrTempo) > 0 THEN (fo.EST_FechamentoOrdValorElaborac + fo.EST_FechamentoOrdValorDespDire + fo.EST_FechamentoOrdValorDespCent) / SUM(fhr.EST_FechamentoOrdHrTempo) ELSE 0 END AS mo_por_hora,
    CASE WHEN fo.EST_FechamentoOrdQuantidade > 0 THEN (fo.EST_FechamentoOrdValorElaborac + fo.EST_FechamentoOrdValorDespDire + fo.EST_FechamentoOrdValorDespCent) / fo.EST_FechamentoOrdQuantidade ELSE 0 END AS mo_por_peca,
    fo.EST_FechamentoOrdValorMP AS custo_mp,
    CASE WHEN fo.EST_FechamentoOrdQuantidade > 0 THEN fo.EST_FechamentoOrdValorMP / fo.EST_FechamentoOrdQuantidade ELSE 0 END AS mp_por_peca,
    CAST(fo.EST_FechamentoData AS DATE) AS data_op,
    CASE WHEN RTRIM(fo.EST_FechamentoOrdProduto) IN (SELECT RTRIM(ProdutoFilho) FROM #BomExplodida WHERE TipoProduto = 'PA') THEN 'PERIODO' ELSE 'ULTIMAS' END AS tipo_filtro
FROM EST_FECHAMENTOORD fo
INNER JOIN EST_FECHAMENTOORDHR fhr 
    ON fhr.EST_FechamentoOrdSequencial = fo.EST_FechamentoOrdSequencial 
   AND fhr.FIL_Codigo = fo.FIL_Codigo
   AND CAST(fhr.EST_FechamentoOrdHrCentro AS VARCHAR) LIKE '2%'
LEFT JOIN CCT_CENTROCUSTO cc ON cc.CCT_Codigo = fhr.EST_FechamentoOrdHrCentro
WHERE fo.EST_FechamentoOrdProduto IN (SELECT ProdutoFilho FROM #BomExplodida)
  AND fo.FIL_Codigo = @FilialCodigo
  AND fo.EST_FechamentoOrdQuantidade > 0
  AND (
    (RTRIM(fo.EST_FechamentoOrdProduto) IN (SELECT RTRIM(ProdutoFilho) FROM #BomExplodida WHERE TipoProduto = 'PA') 
     AND fo.EST_FechamentoData BETWEEN @DataInicio AND DATEADD(DAY, 1, @DataFim))
    OR
    (RTRIM(fo.EST_FechamentoOrdProduto) IN (SELECT RTRIM(ProdutoFilho) FROM #BomExplodida WHERE TipoProduto = 'SA'))
  )
GROUP BY 
    fo.EST_FechamentoOrdProduto,
    fo.EST_FechamentoOrdOrigemNumero,
    cc.CCT_Descricao,
    fhr.EST_FechamentoOrdHrCentro,
    fo.EST_FechamentoOrdQuantidade,
    fo.EST_FechamentoOrdValorElaborac,
    fo.EST_FechamentoOrdValorDespDire,
    fo.EST_FechamentoOrdValorDespCent,
    fo.EST_FechamentoOrdValorMP,
    fo.EST_FechamentoData
ORDER BY produto_codigo, op_numero DESC;

WITH NF_Ranqueada AS (
    SELECT 
        RTRIM(nfi.NFS_NotaFiscalItemProduto) AS produto_codigo,
        RTRIM(nf.NFS_NotaFiscalNumero) AS nf_numero,
        CAST(nf.NFS_NotaFiscalDataEmissao AS DATE) AS data_nf,
        nfi.NFS_NotaFiscalItemQuantidade AS quantidade,
        nfi.NFS_NotaFiscalItemValorTotal AS valor_total,
        ISNULL(icms.NFS_NotaFiscalItemIValor, 0) AS icms,
        nfi.NFS_NotaFiscalItemValorTotal - ISNULL(icms.NFS_NotaFiscalItemIValor, 0) AS valor_compra,
        b.QuantidadeBase AS qtd_utilizada,
        CASE 
            WHEN nfi.NFS_NotaFiscalItemQuantidade > 0 
            THEN ((nfi.NFS_NotaFiscalItemValorTotal - ISNULL(icms.NFS_NotaFiscalItemIValor, 0)) 
                 / nfi.NFS_NotaFiscalItemQuantidade) * b.QuantidadeBase
            ELSE 0 
        END AS mp_por_peca,
        ROW_NUMBER() OVER (
            PARTITION BY nfi.NFS_NotaFiscalItemProduto 
            ORDER BY nf.NFS_NotaFiscalDataEmissao DESC
        ) AS rn
    FROM NFS_NOTAFISCALITEM nfi
    INNER JOIN NFS_NOTAFISCAL nf 
        ON nf.NFS_NotaFiscalFilial = nfi.NFS_NotaFiscalFilial 
       AND nf.NFS_NotaFiscalSeq = nfi.NFS_NotaFiscalSeq
    INNER JOIN #BomExplodida b 
        ON RTRIM(b.ProdutoFilho) = RTRIM(nfi.NFS_NotaFiscalItemProduto) 
       AND b.TemEstrutura = 'N'
    LEFT JOIN NFS_NOTAFISCALITEMI icms 
        ON icms.NFS_NotaFiscalFilial = nfi.NFS_NotaFiscalFilial 
       AND icms.NFS_NotaFiscalSeq = nfi.NFS_NotaFiscalSeq 
       AND icms.NFS_NotaFiscalItemSeq = nfi.NFS_NotaFiscalItemSeq
       AND icms.NFS_NotaFiscalItemIImposto = 1
    WHERE nf.NFS_NotaFiscalTipo = 'E' 
      AND nf.NFS_NotaFiscalSituacao = 'C' 
      AND nf.NFS_NotaFiscalFilial = @FilialCodigo
)
SELECT
    produto_codigo,
    nf_numero,
    data_nf,
    quantidade,
    valor_total,
    icms,
    valor_compra,
    qtd_utilizada,
    mp_por_peca
FROM NF_Ranqueada
WHERE rn <= 5
ORDER BY produto_codigo, data_nf DESC;

DROP TABLE #BomExplodida;
"""

BOM_COST_ROLLUP_SQL = """
DECLARE @ProdutoCodigo CHAR(60) = ?;
DECLARE @TipoCusto CHAR(1) = ?;
DECLARE @QtdNotasMedia INT = ?;
DECLARE @LimiteVariacaoPreco DECIMAL(5,2) = ?;

IF OBJECT_ID('tempdb..#BomExplodida') IS NOT NULL DROP TABLE #BomExplodida;

CREATE TABLE #BomExplodida (
    Nivel INT,
    ProdutoPai CHAR(60),
    ProdutoFilho CHAR(60),
    DescricaoFilho VARCHAR(120),
    QuantidadeNecessaria DECIMAL(18,6),
    PercPerda DECIMAL(18,6),
    QuantidadeComPerda DECIMAL(18,6),
    UnidadeMedida CHAR(6),
    TipoProduto VARCHAR(20),
    TemEstrutura CHAR(1),
    CaminhoHierarquia VARCHAR(MAX),
    NumeroHierarquia VARCHAR(50)
);

;WITH BomRecursiva AS (
    SELECT 
        0 AS Nivel,
        CAST(NULL AS CHAR(60)) AS ProdutoPai,
        p.PRO_Codigo AS ProdutoFilho,
        p.PRO_Descricao AS DescricaoFilho,
        CAST(1.000000 AS DECIMAL(18,6)) AS QuantidadeNecessaria,
        CAST(0.000000 AS DECIMAL(18,6)) AS PercPerda,
        CAST(1.000000 AS DECIMAL(18,6)) AS QuantidadeComPerda,
        p.PRO_UnidadeMedida AS UnidadeMedida,
        CASE 
            WHEN p.PRO_Codigo LIKE 'SA%' THEN 'SEMI-ACABADO'
            WHEN p.PRO_Codigo LIKE 'MP%' THEN 'MATERIA-PRIMA'
            ELSE 'ACABADO'
        END AS TipoProduto,
        CASE WHEN EXISTS (
            SELECT 1 FROM ENG_ESTRUTURA e 
            WHERE e.ENG_EstruturaProduto = p.PRO_Codigo 
            AND e.ENG_EstruturaAtiva = 'S'
        ) THEN 'S' ELSE 'N' END AS TemEstrutura,
        CAST(p.PRO_Codigo AS VARCHAR(MAX)) AS CaminhoHierarquia,
        CAST('000' AS VARCHAR(50)) AS NumeroHierarquia
    FROM PRO_PRODUTO p
    WHERE p.PRO_Codigo = @ProdutoCodigo

    UNION ALL

    SELECT 
        r.Nivel + 1 AS Nivel,
        r.ProdutoFilho AS ProdutoPai,
        ei.ENG_EstruturaItemProduto AS ProdutoFilho,
        p.PRO_Descricao AS DescricaoFilho,
        CAST(r.QuantidadeComPerda * ei.ENG_EstruturaItemQuantidade AS DECIMAL(18,6)) AS QuantidadeNecessaria,
        CAST(ISNULL(ei.ENG_EstruturaItemPerdaPer, 0) AS DECIMAL(18,6)) AS PercPerda,
        CAST(r.QuantidadeComPerda * ei.ENG_EstruturaItemQuantidade * (1 + ISNULL(ei.ENG_EstruturaItemPerdaPer, 0) / 100.0) AS DECIMAL(18,6)) AS QuantidadeComPerda,
        p.PRO_UnidadeMedida AS UnidadeMedida,
        CASE 
            WHEN ei.ENG_EstruturaItemProduto LIKE 'SA%' THEN 'SEMI-ACABADO'
            WHEN ei.ENG_EstruturaItemProduto LIKE 'MP%' THEN 'MATERIA-PRIMA'
            ELSE 'ACABADO'
        END AS TipoProduto,
        CASE WHEN EXISTS (
            SELECT 1 FROM ENG_ESTRUTURA e2 
            WHERE e2.ENG_EstruturaProduto = ei.ENG_EstruturaItemProduto 
            AND e2.ENG_EstruturaAtiva = 'S'
        ) THEN 'S' ELSE 'N' END AS TemEstrutura,
        CAST(r.CaminhoHierarquia + ' > ' + RTRIM(ei.ENG_EstruturaItemProduto) AS VARCHAR(MAX)) AS CaminhoHierarquia,
        CAST(r.NumeroHierarquia + '.' + RIGHT('000' + CAST(ei.ENG_EstruturaItemSeq AS VARCHAR), 3) AS VARCHAR(50)) AS NumeroHierarquia
    FROM BomRecursiva r
    INNER JOIN ENG_ESTRUTURA e 
        ON e.ENG_EstruturaProduto = r.ProdutoFilho
        AND e.ENG_EstruturaAtiva = 'S'
        AND e.ENG_EstruturaTipo = 'P'
    INNER JOIN ENG_ESTRUTURAITEM ei 
        ON ei.ENG_EstruturaProduto = e.ENG_EstruturaProduto
        AND ei.ENG_EstruturaNumero = e.ENG_EstruturaNumero
    INNER JOIN PRO_PRODUTO p 
        ON p.PRO_Codigo = ei.ENG_EstruturaItemProduto
    WHERE r.Nivel < 10
)
INSERT INTO #BomExplodida
SELECT * FROM BomRecursiva;

IF OBJECT_ID('tempdb..#CustosMP') IS NOT NULL DROP TABLE #CustosMP;

CREATE TABLE #CustosMP (
    ProdutoCodigo CHAR(60),
    UltimoCusto DECIMAL(19,9),
    UltimaNF VARCHAR(50),
    PenultimoCusto DECIMAL(19,9),
    VariacaoPreco DECIMAL(10,2),
    CustoMedio DECIMAL(19,9),
    CustoContabil DECIMAL(17,2),
    RefFechamento DECIMAL(18,0),
    CustoFinal DECIMAL(19,9),
    OrigemCusto VARCHAR(20),
    DataUltimaCompra DATETIME,
    QtdNotasEncontradas INT
);

INSERT INTO #CustosMP
SELECT 
    mp.ProdutoFilho,
    (SELECT TOP 1 nfi.NFS_NotaFiscalItemCusto 
     FROM NFS_NOTAFISCALITEM nfi
     INNER JOIN NFS_NOTAFISCAL nf 
         ON nf.NFS_NotaFiscalFilial = nfi.NFS_NotaFiscalFilial
         AND nf.NFS_NotaFiscalSeq = nfi.NFS_NotaFiscalSeq
     WHERE nfi.NFS_NotaFiscalItemProduto = mp.ProdutoFilho
       AND nf.NFS_NotaFiscalTipo = 'E'
       AND nf.NFS_NotaFiscalSituacao = 'C'
       AND nfi.NFS_NotaFiscalItemCusto > 0
     ORDER BY nf.NFS_NotaFiscalDataEmissao DESC
    ) AS UltimoCusto,
    (SELECT TOP 1 RTRIM(nf.NFS_NotaFiscalNumero)
     FROM NFS_NOTAFISCALITEM nfi
     INNER JOIN NFS_NOTAFISCAL nf 
         ON nf.NFS_NotaFiscalFilial = nfi.NFS_NotaFiscalFilial
         AND nf.NFS_NotaFiscalSeq = nfi.NFS_NotaFiscalSeq
     WHERE nfi.NFS_NotaFiscalItemProduto = mp.ProdutoFilho
       AND nf.NFS_NotaFiscalTipo = 'E'
       AND nf.NFS_NotaFiscalSituacao = 'C'
       AND nfi.NFS_NotaFiscalItemCusto > 0
     ORDER BY nf.NFS_NotaFiscalDataEmissao DESC
    ) AS UltimaNF,
    (SELECT TOP 1 sub.Custo
     FROM (
         SELECT nfi.NFS_NotaFiscalItemCusto AS Custo,
                ROW_NUMBER() OVER (ORDER BY nf.NFS_NotaFiscalDataEmissao DESC) AS RowNum
         FROM NFS_NOTAFISCALITEM nfi
         INNER JOIN NFS_NOTAFISCAL nf 
             ON nf.NFS_NotaFiscalFilial = nfi.NFS_NotaFiscalFilial
             AND nf.NFS_NotaFiscalSeq = nfi.NFS_NotaFiscalSeq
         WHERE nfi.NFS_NotaFiscalItemProduto = mp.ProdutoFilho
           AND nf.NFS_NotaFiscalTipo = 'E'
           AND nf.NFS_NotaFiscalSituacao = 'C'
           AND nfi.NFS_NotaFiscalItemCusto > 0
     ) sub
     WHERE sub.RowNum = 2
    ) AS PenultimoCusto,
    NULL AS VariacaoPreco,
    (SELECT AVG(sub.Custo)
     FROM (
         SELECT TOP (@QtdNotasMedia) nfi.NFS_NotaFiscalItemCusto AS Custo
         FROM NFS_NOTAFISCALITEM nfi
         INNER JOIN NFS_NOTAFISCAL nf 
             ON nf.NFS_NotaFiscalFilial = nfi.NFS_NotaFiscalFilial
             AND nf.NFS_NotaFiscalSeq = nfi.NFS_NotaFiscalSeq
         WHERE nfi.NFS_NotaFiscalItemProduto = mp.ProdutoFilho
           AND nf.NFS_NotaFiscalTipo = 'E'
           AND nf.NFS_NotaFiscalSituacao = 'C'
           AND nfi.NFS_NotaFiscalItemCusto > 0
         ORDER BY nf.NFS_NotaFiscalDataEmissao DESC
     ) sub
    ) AS CustoMedio,
    (SELECT TOP 1 fc.EST_FechamentoCstCustoMedio
     FROM EST_FECHAMENTOCUSTO fc
     WHERE fc.EST_FechamentoCstProduto = mp.ProdutoFilho
       AND fc.EST_FechamentoCstCustoMedio > 0
     ORDER BY fc.EST_FechamentoCstReferencia DESC
    ) AS CustoContabil,
    (SELECT TOP 1 fc.EST_FechamentoCstReferencia
     FROM EST_FECHAMENTOCUSTO fc
     WHERE fc.EST_FechamentoCstProduto = mp.ProdutoFilho
       AND fc.EST_FechamentoCstCustoMedio > 0
     ORDER BY fc.EST_FechamentoCstReferencia DESC
    ) AS RefFechamento,
    NULL AS CustoFinal,
    NULL AS OrigemCusto,
    (SELECT TOP 1 nf.NFS_NotaFiscalDataEmissao 
     FROM NFS_NOTAFISCALITEM nfi
     INNER JOIN NFS_NOTAFISCAL nf 
         ON nf.NFS_NotaFiscalFilial = nfi.NFS_NotaFiscalFilial
         AND nf.NFS_NotaFiscalSeq = nfi.NFS_NotaFiscalSeq
     WHERE nfi.NFS_NotaFiscalItemProduto = mp.ProdutoFilho
       AND nf.NFS_NotaFiscalTipo = 'E'
       AND nf.NFS_NotaFiscalSituacao = 'C'
     ORDER BY nf.NFS_NotaFiscalDataEmissao DESC
    ) AS DataUltimaCompra,
    (SELECT COUNT(DISTINCT nf.NFS_NotaFiscalSeq)
     FROM NFS_NOTAFISCALITEM nfi
     INNER JOIN NFS_NOTAFISCAL nf 
         ON nf.NFS_NotaFiscalFilial = nfi.NFS_NotaFiscalFilial
         AND nf.NFS_NotaFiscalSeq = nfi.NFS_NotaFiscalSeq
     WHERE nfi.NFS_NotaFiscalItemProduto = mp.ProdutoFilho
       AND nf.NFS_NotaFiscalTipo = 'E'
       AND nf.NFS_NotaFiscalSituacao = 'C'
    ) AS QtdNotasEncontradas
FROM (
    SELECT DISTINCT ProdutoFilho 
    FROM #BomExplodida 
    WHERE TemEstrutura = 'N'
) mp;

UPDATE #CustosMP
SET VariacaoPreco = CASE 
        WHEN PenultimoCusto IS NOT NULL AND PenultimoCusto > 0 
        THEN ((UltimoCusto - PenultimoCusto) / PenultimoCusto) * 100
        ELSE NULL 
    END;

UPDATE #CustosMP
SET CustoFinal = CASE 
        WHEN UltimoCusto IS NOT NULL THEN UltimoCusto
        WHEN CustoContabil IS NOT NULL THEN CustoContabil
        ELSE 0
    END,
    OrigemCusto = CASE 
        WHEN UltimoCusto IS NOT NULL THEN 'NOTA'
        WHEN CustoContabil IS NOT NULL THEN 'CONTABIL'
        ELSE 'SEM CUSTO'
    END;

SELECT 
    RTRIM(b.ProdutoFilho) AS produto_codigo,
    b.DescricaoFilho AS produto_descricao,
    b.NumeroHierarquia AS numero_hierarquia,
    b.Nivel AS nivel,
    b.TipoProduto AS tipo_produto,
    b.QuantidadeComPerda AS quantidade_necessaria,
    b.UnidadeMedida AS unidade_medida,
    CASE 
        WHEN b.TemEstrutura = 'N' THEN 
            CASE @TipoCusto 
                WHEN 'U' THEN c.CustoFinal 
                ELSE ISNULL(c.CustoMedio, c.CustoFinal) 
            END
        ELSE NULL 
    END AS custo_unitario,
    CASE 
        WHEN b.TemEstrutura = 'N' THEN 
            b.QuantidadeComPerda * CASE @TipoCusto 
                WHEN 'U' THEN ISNULL(c.CustoFinal, 0) 
                ELSE ISNULL(c.CustoMedio, ISNULL(c.CustoFinal, 0)) 
            END
        ELSE NULL 
    END AS custo_total,
    c.OrigemCusto AS origem_custo,
    c.VariacaoPreco AS variacao_preco,
    CASE 
        WHEN c.VariacaoPreco IS NOT NULL AND ABS(c.VariacaoPreco) > @LimiteVariacaoPreco 
        THEN 'VAR ' + CAST(CAST(c.VariacaoPreco AS DECIMAL(10,1)) AS VARCHAR) + '%'
        WHEN b.TemEstrutura = 'N' AND c.CustoFinal IS NULL THEN 'SEM CUSTO'
        WHEN c.OrigemCusto = 'CONTABIL' THEN 'CONTABIL'
        ELSE 'OK' 
    END AS status_alerta,
    CAST(ISNULL(c.UltimaNF, '') AS VARCHAR(50)) AS ultima_nf,
    CAST(ISNULL(c.DataUltimaCompra, GETDATE()) AS DATETIME) AS data_ultima_compra,
    ISNULL(c.QtdNotasEncontradas, 0) AS qtd_notas_encontradas
FROM #BomExplodida b
LEFT JOIN #CustosMP c ON c.ProdutoCodigo = b.ProdutoFilho
ORDER BY b.NumeroHierarquia;

DROP TABLE #BomExplodida;
DROP TABLE #CustosMP;
"""


class SyncService:
    """Executa carga de dados do SQL Server para o PostgreSQL de cache."""

    def __init__(self, source_db: Session, app_db: Session):
        self.source_db = source_db
        self.app_db = app_db

    def ensure_tables(self) -> None:
        """Cria as tabelas de cache e controle caso ainda nao existam."""
        self.app_db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sync_runs (
                    id BIGSERIAL PRIMARY KEY,
                    started_at TIMESTAMPTZ NOT NULL,
                    finished_at TIMESTAMPTZ,
                    status VARCHAR(20) NOT NULL,
                    records_loaded INTEGER NOT NULL DEFAULT 0,
                    error_message TEXT,
                    payload JSONB
                );
                """
            )
        )

        self.app_db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS product_cost_snapshot (
                    product_code VARCHAR(100) NOT NULL,
                    filial VARCHAR(20),
                    data_fechamento DATE NOT NULL,
                    data_custo DATE,
                    custo_medio_real NUMERIC(18, 6),
                    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (product_code, filial, data_fechamento)
                );
                """
            )
        )

        self.app_db.execute(
            text(
                """
                ALTER TABLE product_cost_snapshot
                ADD COLUMN IF NOT EXISTS data_fechamento DATE;
                """
            )
        )
        self.app_db.execute(
            text(
                """
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'product_cost_snapshot'
                          AND column_name = 'mes_fechamento'
                    ) THEN
                        UPDATE product_cost_snapshot
                        SET data_fechamento = COALESCE(data_fechamento, mes_fechamento);

                        ALTER TABLE product_cost_snapshot
                        ALTER COLUMN data_fechamento SET NOT NULL;

                        ALTER TABLE product_cost_snapshot
                        DROP COLUMN mes_fechamento;
                    END IF;
                END $$;
                """
            )
        )
        self.app_db.execute(
            text(
                """
                UPDATE product_cost_snapshot
                SET data_fechamento = COALESCE(data_fechamento, CURRENT_DATE);
                """
            )
        )
        self.app_db.execute(
            text(
                """
                ALTER TABLE product_cost_snapshot
                ALTER COLUMN data_fechamento SET NOT NULL;
                """
            )
        )
        self.app_db.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS product_cost_snapshot_pk_idx
                ON product_cost_snapshot (product_code, filial, data_fechamento);
                """
            )
        )

        self.app_db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS product_nf_cost_analysis (
                    product_code VARCHAR(60) NOT NULL,
                    descricao VARCHAR(120),
                    qtd_estoque NUMERIC(18, 2),
                    custo_medio_fech NUMERIC(18, 4),
                    valor_total_estoque NUMERIC(18, 2),
                    qtd_nfs INTEGER,
                    qtd_comprada_nfs NUMERIC(18, 2),
                    custo_medio_pond_nfs NUMERIC(18, 4),
                    menor_custo_nf NUMERIC(18, 4),
                    maior_custo_nf NUMERIC(18, 4),
                    ultimo_custo_nf NUMERIC(18, 4),
                    data_ultima_nf DATE,
                    dif_pct_fech_vs_nf NUMERIC(18, 2),
                    status VARCHAR(17),
                    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (product_code)
                );
                """
            )
        )

        self.app_db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS product_update_status (
                    product_code VARCHAR(100) PRIMARY KEY,
                    last_sync_at TIMESTAMPTZ NOT NULL,
                    last_data_fechamento DATE,
                    source VARCHAR(50) NOT NULL DEFAULT 'sqlserver'
                );
                """
            )
        )
        self.app_db.commit()

    def run_sync(
        self,
        data_fechamento: str,
        data_inicio_nf: str,
        product_codes: Optional[List[str]] = None,
        datasets: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Executa o processo completo de sincronizacao dos datasets."""
        self.ensure_tables()
        product_codes = product_codes or settings.sync_product_codes_list
        dataset_set = {name.strip().lower() for name in (datasets or []) if name.strip()}
        if not dataset_set:
            dataset_set = {"custo_contabil", "custo_nf", "cost_map", "bom_rollup"}

        data_fechamento_date = self._parse_date(data_fechamento)
        data_inicio_cost_map = date(data_fechamento_date.year, 1, 1)
        data_fim_cost_map = data_fechamento_date

        run_start = datetime.now(timezone.utc)
        run_id = self._start_run(run_start, data_fechamento, data_inicio_nf)

        try:
            custo_contabil_rows: List[Dict[str, Any]] = []
            custo_nf_rows: List[Dict[str, Any]] = []
            loaded_1 = 0
            loaded_2 = 0
            cost_map_loaded = 0
            bom_loaded = 0

            if "custo_contabil" in dataset_set:
                custo_contabil_rows = self._extract_custo_contabil(product_codes)
                loaded_1 = self._load_custo_contabil(custo_contabil_rows)
                self._touch_product_status(custo_contabil_rows, data_fechamento)

            if "custo_nf" in dataset_set:
                custo_nf_rows = self._extract_custo_nf(data_fechamento, data_inicio_nf, product_codes)
                loaded_2 = self._load_custo_nf(custo_nf_rows, product_codes)
                self._touch_product_status(custo_nf_rows, data_fechamento)

            if "cost_map" in dataset_set:
                for product_code in product_codes:
                    cost_map_loaded += self._run_cost_map_for_product(
                        product_code=product_code,
                        data_inicio=data_inicio_cost_map,
                        data_fim=data_fim_cost_map,
                        filial_codigo=settings.cost_map_filial_codigo,
                    )

            if "bom_rollup" in dataset_set:
                for product_code in product_codes:
                    bom_loaded += self._run_bom_cost_rollup_for_product(
                        product_code=product_code,
                        tipo_custo=settings.bom_tipo_custo,
                        qtd_notas_media=settings.bom_qtd_notas_media,
                        limite_variacao_preco=settings.bom_limite_variacao_preco,
                        data_referencia=data_fechamento_date,
                    )

            total_loaded = loaded_1 + loaded_2 + cost_map_loaded + bom_loaded
            
            # Se nenhum registro foi carregado, pode ser que as tabelas SQL Server não têm dados
            if total_loaded == 0:
                logger.warning("⚠ Sincronização concluída, mas nenhum registro foi carregado")
                logger.warning("→ Verifique se SQL Server tem dados nas tabelas de origem")
                self._finish_run(run_id, "success_empty", 0, None)
            else:
                logger.info(f"✓ Sincronização concluída com sucesso: {total_loaded} registros")
                self._finish_run(run_id, "success", total_loaded, None)
            
            self.app_db.commit()

            return {
                "status": "success",
                "records_loaded": total_loaded,
                "data_fechamento": data_fechamento,
                "data_inicio_nf": data_inicio_nf,
                "message": "Sincronização concluída" if total_loaded > 0 else "Sincronização concluída, mas sem dados da origem"
            }
        except Exception as exc:
            self.app_db.rollback()
            logger.error(f"✗ Erro na sincronização: {str(exc)}")
            self._finish_run(run_id, "failed", 0, str(exc))
            self.app_db.commit()
            raise

    def get_last_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        rows = self.app_db.execute(
            text(
                """
                SELECT id, started_at, finished_at, status, records_loaded, error_message, payload
                FROM sync_runs
                ORDER BY id DESC
                LIMIT :limit;
                """
            ),
            {"limit": limit},
        ).mappings().all()
        return [dict(row) for row in rows]

    def get_product_status(self, limit: int = 200) -> List[Dict[str, Any]]:
        rows = self.app_db.execute(
            text(
                """
                SELECT product_code, last_sync_at, last_data_fechamento, source
                FROM product_update_status
                ORDER BY last_sync_at DESC
                LIMIT :limit;
                """
            ),
            {"limit": limit},
        ).mappings().all()
        return [dict(row) for row in rows]

    def _parse_date(self, value: str) -> date:
        return datetime.strptime(value, "%Y-%m-%d").date()

    def _run_cost_map_for_product(
        self,
        product_code: str,
        data_inicio: date,
        data_fim: date,
        filial_codigo: int,
    ) -> int:
        started_at = datetime.now(timezone.utc)
        try:
            bom_rows, ops_rows, nfs_rows = self._extract_cost_map(
                product_code, data_inicio, data_fim, filial_codigo
            )
            loaded_bom = self._load_cost_map_bom(bom_rows)
            loaded_ops = self._load_cost_map_ops(ops_rows)
            loaded_nfs = self._load_cost_map_nfs(nfs_rows)
            total_loaded = loaded_bom + loaded_ops + loaded_nfs
            self._insert_cost_map_run(
                product_code=product_code,
                data_inicio=data_inicio,
                data_fim=data_fim,
                filial_codigo=filial_codigo,
                estrutura_gerada=loaded_bom,
                ops_encontradas=loaded_ops,
                nfs_encontradas=loaded_nfs,
                status="success",
                error_message=None,
                started_at=started_at,
            )
            return total_loaded
        except Exception as exc:
            self._insert_cost_map_run(
                product_code=product_code,
                data_inicio=data_inicio,
                data_fim=data_fim,
                filial_codigo=filial_codigo,
                estrutura_gerada=0,
                ops_encontradas=0,
                nfs_encontradas=0,
                status="failed",
                error_message=str(exc),
                started_at=started_at,
            )
            raise

    def _extract_cost_map(
        self,
        product_code: str,
        data_inicio: date,
        data_fim: date,
        filial_codigo: int,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        connection = self.source_db.connection().connection
        cursor = connection.cursor()
        cursor.execute(COST_MAP_SQL, (product_code, data_inicio, data_fim, filial_codigo))

        def _fetch_rows(cur) -> List[Dict[str, Any]]:
            if not cur.description:
                return []
            columns = [col[0] for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

        bom_rows = _fetch_rows(cursor)
        ops_rows: List[Dict[str, Any]] = []
        nfs_rows: List[Dict[str, Any]] = []
        if cursor.nextset():
            ops_rows = _fetch_rows(cursor)
        if cursor.nextset():
            nfs_rows = _fetch_rows(cursor)

        cursor.close()
        return bom_rows, ops_rows, nfs_rows

    def _load_cost_map_bom(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        for row in rows:
            self.app_db.execute(
                text(
                    """
                    INSERT INTO cost_map_bom_structure (
                        produto_codigo,
                        produto_descricao,
                        tipo_produto,
                        tem_estrutura,
                        numero_hierarquia,
                        nivel,
                        pai_codigo,
                        quantidade_base,
                        loaded_at
                    ) VALUES (
                        :produto_codigo,
                        :produto_descricao,
                        :tipo_produto,
                        :tem_estrutura,
                        :numero_hierarquia,
                        :nivel,
                        :pai_codigo,
                        :quantidade_base,
                        NOW()
                    )
                    ON CONFLICT (produto_codigo, numero_hierarquia)
                    DO UPDATE SET
                        produto_descricao = EXCLUDED.produto_descricao,
                        tipo_produto = EXCLUDED.tipo_produto,
                        tem_estrutura = EXCLUDED.tem_estrutura,
                        nivel = EXCLUDED.nivel,
                        pai_codigo = EXCLUDED.pai_codigo,
                        quantidade_base = EXCLUDED.quantidade_base,
                        loaded_at = NOW();
                    """
                ),
                {
                    "produto_codigo": row.get("produto_codigo"),
                    "produto_descricao": row.get("produto_descricao"),
                    "tipo_produto": row.get("tipo_produto"),
                    "tem_estrutura": row.get("tem_estrutura"),
                    "numero_hierarquia": row.get("numero_hierarquia"),
                    "nivel": row.get("nivel"),
                    "pai_codigo": row.get("pai_codigo"),
                    "quantidade_base": row.get("quantidade_base"),
                },
            )

        return len(rows)

    def _load_cost_map_ops(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        inserted = 0
        skipped = 0
        for row in rows:
            if not row.get("op_numero"):
                skipped += 1
                continue
            self.app_db.execute(
                text(
                    """
                    INSERT INTO cost_map_ops (
                        produto_codigo,
                        op_numero,
                        centro_custo,
                        centro_codigo,
                        quantidade,
                        tempo_horas,
                        qtd_por_hora,
                        custo_mo,
                        mo_por_hora,
                        mo_por_peca,
                        custo_mp,
                        mp_por_peca,
                        data_op,
                        tipo_filtro,
                        loaded_at
                    ) VALUES (
                        :produto_codigo,
                        :op_numero,
                        :centro_custo,
                        :centro_codigo,
                        :quantidade,
                        :tempo_horas,
                        :qtd_por_hora,
                        :custo_mo,
                        :mo_por_hora,
                        :mo_por_peca,
                        :custo_mp,
                        :mp_por_peca,
                        :data_op,
                        :tipo_filtro,
                        NOW()
                    )
                    ON CONFLICT (produto_codigo, op_numero, centro_custo)
                    DO UPDATE SET
                        centro_codigo = EXCLUDED.centro_codigo,
                        quantidade = EXCLUDED.quantidade,
                        tempo_horas = EXCLUDED.tempo_horas,
                        qtd_por_hora = EXCLUDED.qtd_por_hora,
                        custo_mo = EXCLUDED.custo_mo,
                        mo_por_hora = EXCLUDED.mo_por_hora,
                        mo_por_peca = EXCLUDED.mo_por_peca,
                        custo_mp = EXCLUDED.custo_mp,
                        mp_por_peca = EXCLUDED.mp_por_peca,
                        data_op = EXCLUDED.data_op,
                        tipo_filtro = EXCLUDED.tipo_filtro,
                        loaded_at = NOW();
                    """
                ),
                {
                    "produto_codigo": row.get("produto_codigo"),
                    "op_numero": row.get("op_numero"),
                    "centro_custo": row.get("centro_custo"),
                    "centro_codigo": row.get("centro_codigo"),
                    "quantidade": row.get("quantidade"),
                    "tempo_horas": row.get("tempo_horas"),
                    "qtd_por_hora": row.get("qtd_por_hora"),
                    "custo_mo": row.get("custo_mo"),
                    "mo_por_hora": row.get("mo_por_hora"),
                    "mo_por_peca": row.get("mo_por_peca"),
                    "custo_mp": row.get("custo_mp"),
                    "mp_por_peca": row.get("mp_por_peca"),
                    "data_op": row.get("data_op"),
                    "tipo_filtro": row.get("tipo_filtro"),
                },
            )

            inserted += 1

        if skipped:
            logger.warning(
                "⚠ Ignoradas %s linhas de OP sem op_numero", skipped
            )
        return inserted

    def _load_cost_map_nfs(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        inserted = 0
        skipped = 0
        skipped_products: set[str] = set()
        for row in rows:
            if not row.get("nf_numero"):
                skipped += 1
                product_code = row.get("produto_codigo")
                if product_code:
                    skipped_products.add(str(product_code))
                continue
            self.app_db.execute(
                text(
                    """
                    INSERT INTO cost_map_nfs (
                        produto_codigo,
                        nf_numero,
                        data_nf,
                        quantidade,
                        valor_total,
                        icms,
                        valor_compra,
                        qtd_utilizada,
                        mp_por_peca,
                        loaded_at
                    ) VALUES (
                        :produto_codigo,
                        :nf_numero,
                        :data_nf,
                        :quantidade,
                        :valor_total,
                        :icms,
                        :valor_compra,
                        :qtd_utilizada,
                        :mp_por_peca,
                        NOW()
                    )
                    ON CONFLICT (produto_codigo, nf_numero)
                    DO UPDATE SET
                        data_nf = EXCLUDED.data_nf,
                        quantidade = EXCLUDED.quantidade,
                        valor_total = EXCLUDED.valor_total,
                        icms = EXCLUDED.icms,
                        valor_compra = EXCLUDED.valor_compra,
                        qtd_utilizada = EXCLUDED.qtd_utilizada,
                        mp_por_peca = EXCLUDED.mp_por_peca,
                        loaded_at = NOW();
                    """
                ),
                {
                    "produto_codigo": row.get("produto_codigo"),
                    "nf_numero": row.get("nf_numero"),
                    "data_nf": row.get("data_nf"),
                    "quantidade": row.get("quantidade"),
                    "valor_total": row.get("valor_total"),
                    "icms": row.get("icms"),
                    "valor_compra": row.get("valor_compra"),
                    "qtd_utilizada": row.get("qtd_utilizada"),
                    "mp_por_peca": row.get("mp_por_peca"),
                },
            )

            inserted += 1

        if skipped:
            logger.warning(
                "⚠ Ignoradas %s linhas de NF sem nf_numero (produtos: %s)",
                skipped,
                ", ".join(sorted(skipped_products)) if skipped_products else "n/a",
            )

        return inserted

    def _insert_cost_map_run(
        self,
        product_code: str,
        data_inicio: date,
        data_fim: date,
        filial_codigo: int,
        estrutura_gerada: int,
        ops_encontradas: int,
        nfs_encontradas: int,
        status: str,
        error_message: Optional[str],
        started_at: datetime,
    ) -> None:
        self.app_db.execute(
            text(
                """
                INSERT INTO cost_map_runs (
                    produto_codigo,
                    data_inicio,
                    data_fim,
                    filial_codigo,
                    estrutura_gerada,
                    ops_encontradas,
                    nfs_encontradas,
                    status,
                    error_message,
                    started_at,
                    finished_at
                ) VALUES (
                    :produto_codigo,
                    :data_inicio,
                    :data_fim,
                    :filial_codigo,
                    :estrutura_gerada,
                    :ops_encontradas,
                    :nfs_encontradas,
                    :status,
                    :error_message,
                    :started_at,
                    NOW()
                );
                """
            ),
            {
                "produto_codigo": product_code,
                "data_inicio": data_inicio,
                "data_fim": data_fim,
                "filial_codigo": filial_codigo,
                "estrutura_gerada": estrutura_gerada,
                "ops_encontradas": ops_encontradas,
                "nfs_encontradas": nfs_encontradas,
                "status": status,
                "error_message": error_message,
                "started_at": started_at,
            },
        )

    def _run_bom_cost_rollup_for_product(
        self,
        product_code: str,
        tipo_custo: str,
        qtd_notas_media: int,
        limite_variacao_preco: float,
        data_referencia: date,
    ) -> int:
        rows = self._extract_bom_cost_rollup(
            product_code, tipo_custo, qtd_notas_media, limite_variacao_preco
        )
        loaded_structure = self._load_bom_structure(rows)
        loaded_summary = self._load_bom_summary(product_code, rows, tipo_custo)
        loaded_alerts = self._load_bom_alerts(rows, data_referencia)
        return loaded_structure + loaded_summary + loaded_alerts

    def _extract_bom_cost_rollup(
        self,
        product_code: str,
        tipo_custo: str,
        qtd_notas_media: int,
        limite_variacao_preco: float,
    ) -> List[Dict[str, Any]]:
        connection = self.source_db.connection().connection
        cursor = connection.cursor()
        cursor.execute(
            BOM_COST_ROLLUP_SQL,
            (product_code, tipo_custo, qtd_notas_media, limite_variacao_preco),
        )

        rows: List[Dict[str, Any]] = []
        while True:
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            if not cursor.nextset():
                break

        cursor.close()
        return rows

    def _load_bom_structure(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        for row in rows:
            self.app_db.execute(
                text(
                    """
                    INSERT INTO product_bom_structure (
                        produto_codigo,
                        produto_descricao,
                        numero_hierarquia,
                        nivel,
                        tipo_produto,
                        quantidade_necessaria,
                        unidade_medida,
                        custo_unitario,
                        custo_total,
                        origem_custo,
                        variacao_preco,
                        status_alerta,
                        ultima_nf,
                        data_ultima_compra,
                        qtd_notas_encontradas,
                        loaded_at
                    ) VALUES (
                        :produto_codigo,
                        :produto_descricao,
                        :numero_hierarquia,
                        :nivel,
                        :tipo_produto,
                        :quantidade_necessaria,
                        :unidade_medida,
                        :custo_unitario,
                        :custo_total,
                        :origem_custo,
                        :variacao_preco,
                        :status_alerta,
                        :ultima_nf,
                        :data_ultima_compra,
                        :qtd_notas_encontradas,
                        NOW()
                    )
                    ON CONFLICT (produto_codigo, numero_hierarquia)
                    DO UPDATE SET
                        produto_descricao = EXCLUDED.produto_descricao,
                        nivel = EXCLUDED.nivel,
                        tipo_produto = EXCLUDED.tipo_produto,
                        quantidade_necessaria = EXCLUDED.quantidade_necessaria,
                        unidade_medida = EXCLUDED.unidade_medida,
                        custo_unitario = EXCLUDED.custo_unitario,
                        custo_total = EXCLUDED.custo_total,
                        origem_custo = EXCLUDED.origem_custo,
                        variacao_preco = EXCLUDED.variacao_preco,
                        status_alerta = EXCLUDED.status_alerta,
                        ultima_nf = EXCLUDED.ultima_nf,
                        data_ultima_compra = EXCLUDED.data_ultima_compra,
                        qtd_notas_encontradas = EXCLUDED.qtd_notas_encontradas,
                        loaded_at = NOW();
                    """
                ),
                {
                    "produto_codigo": row.get("produto_codigo"),
                    "produto_descricao": row.get("produto_descricao"),
                    "numero_hierarquia": row.get("numero_hierarquia"),
                    "nivel": row.get("nivel"),
                    "tipo_produto": row.get("tipo_produto"),
                    "quantidade_necessaria": row.get("quantidade_necessaria"),
                    "unidade_medida": row.get("unidade_medida"),
                    "custo_unitario": row.get("custo_unitario"),
                    "custo_total": row.get("custo_total"),
                    "origem_custo": row.get("origem_custo"),
                    "variacao_preco": row.get("variacao_preco"),
                    "status_alerta": row.get("status_alerta"),
                    "ultima_nf": row.get("ultima_nf"),
                    "data_ultima_compra": row.get("data_ultima_compra"),
                    "qtd_notas_encontradas": row.get("qtd_notas_encontradas"),
                },
            )

        return len(rows)

    def _load_bom_summary(self, product_code: str, rows: List[Dict[str, Any]], tipo_custo: str) -> int:
        if not rows:
            return 0

        root_row = next((r for r in rows if r.get("numero_hierarquia") == "000"), rows[0])
        produto_descricao = root_row.get("produto_descricao") if root_row else None

        tipo_custo_normalized = tipo_custo.upper() if tipo_custo else ""
        if tipo_custo_normalized in ("1", "U"):
            metodo_custeio = "ULTIMO_CUSTO"
        elif tipo_custo_normalized in ("2", "M"):
            metodo_custeio = "CUSTO_MEDIO"
        else:
            metodo_custeio = "CUSTO_MEDIO"

        materias_primas = [
            r for r in rows if (r.get("tipo_produto") or "").upper() == "MATERIA-PRIMA"
        ]
        custo_total_materia_prima = sum(
            r.get("custo_total") or 0 for r in materias_primas
        )
        qtd_materia_primas_distintas = len(
            {r.get("produto_codigo") for r in materias_primas if r.get("produto_codigo")}
        )
        qtd_itens_sem_custo = len(
            [r for r in rows if (r.get("status_alerta") or "").upper() == "SEM CUSTO"]
        )
        qtd_itens_com_custo_contabil = len(
            [r for r in rows if (r.get("origem_custo") or "").upper() == "CONTABIL"]
        )
        qtd_itens_com_variacao_alta = len(
            [r for r in rows if (r.get("status_alerta") or "").startswith("VAR ")]
        )

        self.app_db.execute(
            text(
                """
                INSERT INTO product_bom_cost_summary (
                    produto_codigo,
                    produto_descricao,
                    tipo_custo_usado,
                    metodo_custeio,
                    custo_total_materia_prima,
                    qtd_materia_primas_distintas,
                    qtd_itens_sem_custo,
                    qtd_itens_com_custo_contabil,
                    qtd_itens_com_variacao_alta,
                    data_simulacao,
                    loaded_at
                ) VALUES (
                    :produto_codigo,
                    :produto_descricao,
                    :tipo_custo_usado,
                    :metodo_custeio,
                    :custo_total_materia_prima,
                    :qtd_materia_primas_distintas,
                    :qtd_itens_sem_custo,
                    :qtd_itens_com_custo_contabil,
                    :qtd_itens_com_variacao_alta,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (produto_codigo)
                DO UPDATE SET
                    produto_descricao = EXCLUDED.produto_descricao,
                    tipo_custo_usado = EXCLUDED.tipo_custo_usado,
                    metodo_custeio = EXCLUDED.metodo_custeio,
                    custo_total_materia_prima = EXCLUDED.custo_total_materia_prima,
                    qtd_materia_primas_distintas = EXCLUDED.qtd_materia_primas_distintas,
                    qtd_itens_sem_custo = EXCLUDED.qtd_itens_sem_custo,
                    qtd_itens_com_custo_contabil = EXCLUDED.qtd_itens_com_custo_contabil,
                    qtd_itens_com_variacao_alta = EXCLUDED.qtd_itens_com_variacao_alta,
                    data_simulacao = NOW(),
                    loaded_at = NOW();
                """
            ),
            {
                "produto_codigo": product_code,
                "produto_descricao": produto_descricao,
                "tipo_custo_usado": tipo_custo,
                "metodo_custeio": metodo_custeio,
                "custo_total_materia_prima": custo_total_materia_prima,
                "qtd_materia_primas_distintas": qtd_materia_primas_distintas,
                "qtd_itens_sem_custo": qtd_itens_sem_custo,
                "qtd_itens_com_custo_contabil": qtd_itens_com_custo_contabil,
                "qtd_itens_com_variacao_alta": qtd_itens_com_variacao_alta,
            },
        )
        return 1

    def _load_bom_alerts(self, rows: List[Dict[str, Any]], data_referencia: date) -> int:
        if not rows:
            return 0

        alert_rows = [
            r
            for r in rows
            if (r.get("status_alerta") or "").strip() == "SEM CUSTO"
            or str(r.get("status_alerta") or "").startswith("VAR ")
        ]
        if not alert_rows:
            return 0

        produto_codes = sorted({r.get("produto_codigo") for r in alert_rows if r.get("produto_codigo")})
        if produto_codes:
            delete_query = text(
                """
                DELETE FROM product_bom_alerts
                WHERE produto_codigo IN :produto_codes
                  AND data_referencia = :data_referencia;
                """
            ).bindparams(bindparam("produto_codes", expanding=True))
            self.app_db.execute(
                delete_query,
                {"produto_codes": produto_codes, "data_referencia": data_referencia},
            )

        inserted = 0
        for row in alert_rows:
            status_alerta = (row.get("status_alerta") or "").strip()
            if not status_alerta:
                continue
            if status_alerta.startswith("VAR "):
                tipo_alerta = "VARIACAO"
                severidade = "ALTA"
            else:
                tipo_alerta = "SEM_CUSTO"
                severidade = "MEDIA"

            self.app_db.execute(
                text(
                    """
                    INSERT INTO product_bom_alerts (
                        produto_codigo,
                        tipo_alerta,
                        descricao,
                        custo_anterior,
                        custo_atual,
                        variacao_percentual,
                        mensagem_alerta,
                        severidade,
                        data_referencia,
                        loaded_at
                    ) VALUES (
                        :produto_codigo,
                        :tipo_alerta,
                        :descricao,
                        :custo_anterior,
                        :custo_atual,
                        :variacao_percentual,
                        :mensagem_alerta,
                        :severidade,
                        :data_referencia,
                        NOW()
                    );
                    """
                ),
                {
                    "produto_codigo": row.get("produto_codigo"),
                    "tipo_alerta": tipo_alerta,
                    "descricao": row.get("produto_descricao"),
                    "custo_anterior": None,
                    "custo_atual": row.get("custo_unitario"),
                    "variacao_percentual": row.get("variacao_preco"),
                    "mensagem_alerta": status_alerta,
                    "severidade": severidade,
                    "data_referencia": data_referencia,
                },
            )
            inserted += 1

        return inserted

    def _start_run(self, started_at: datetime, data_fechamento: str, data_inicio_nf: str) -> int:
        row = self.app_db.execute(
            text(
                """
                INSERT INTO sync_runs (started_at, status, payload)
                VALUES (:started_at, 'running', CAST(:payload AS JSONB))
                RETURNING id;
                """
            ),
            {
                "started_at": started_at,
                "payload": (
                    '{"data_fechamento": "'
                    + data_fechamento
                    + '", "data_inicio_nf": "'
                    + data_inicio_nf
                    + '"}'
                ),
            },
        ).first()
        return int(row[0])

    def _finish_run(
        self,
        run_id: int,
        status: str,
        records_loaded: int,
        error_message: Optional[str],
    ) -> None:
        self.app_db.execute(
            text(
                """
                UPDATE sync_runs
                SET finished_at = NOW(),
                    status = :status,
                    records_loaded = :records_loaded,
                    error_message = :error_message
                WHERE id = :run_id;
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "records_loaded": records_loaded,
                "error_message": error_message,
            },
        )

    def _extract_custo_contabil(self, product_codes: List[str]) -> List[Dict[str, Any]]:
        """Extrai dados de custo contabil do SQL Server com tratamento de erro."""
        filter_clause = ""
        if product_codes:
            filter_clause = " AND TRIM(CUS_ValorProdutoCodigo) IN :product_codes"
        query = text(
            f"""
            WITH FechamentoMensal AS (
                SELECT
                    TRIM(CUS_ValorProdutoCodigo) AS CodigoProduto,
                    CUS_ValorProdutoFilial AS Filial,
                    EOMONTH(CUS_ValorProdutoData) AS MesFechamento,
                    CUS_ValorProdutoData AS DataCusto,
                    CUS_ValorProdutoValor AS CustoMedioReal,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            TRIM(CUS_ValorProdutoCodigo),
                            CUS_ValorProdutoFilial,
                            EOMONTH(CUS_ValorProdutoData)
                        ORDER BY CUS_ValorProdutoData DESC
                    ) AS rn
                FROM CUS_VALORPRODUTO
                WHERE CUS_TipoValorCodigo = 1{filter_clause}
            )
            SELECT CodigoProduto, Filial, MesFechamento, DataCusto, CustoMedioReal
            FROM FechamentoMensal
            WHERE rn = 1
            ORDER BY MesFechamento DESC, CodigoProduto;
            """
        )
        if product_codes:
            query = query.bindparams(bindparam("product_codes", expanding=True))
        
        try:
            params: Dict[str, Any] = {}
            if product_codes:
                params["product_codes"] = product_codes
            rows = self.source_db.execute(query, params).mappings().all()
            logger.info(f"✓ Extraídos {len(rows)} registros de custo contábil")
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"⚠ Erro ao extrair custo contábil: {str(e)}")
            logger.warning("→ Retornando lista vazia (tabela pode não existir ou credenciais inválidas)")
            return []

    def _extract_custo_nf(
        self,
        data_fechamento: str,
        data_inicio_nf: str,
        product_codes: List[str],
    ) -> List[Dict[str, Any]]:
        """Extrai dados de custo NF do SQL Server com tratamento de erro."""
        filter_cf = ""
        filter_nf = ""
        if product_codes:
            filter_cf = " AND RTRIM(fi.EST_FechamentoItemProduto) IN :product_codes"
            filter_nf = " AND RTRIM(nfi.NFS_NotaFiscalItemProduto) IN :product_codes"
        query = text(
            f"""
            ;WITH CustoFechamento AS (
                SELECT
                    RTRIM(fi.EST_FechamentoItemProduto) AS Produto,
                    fi.EST_FechamentoItemQtd AS QtdEstoque,
                    fi.EST_FechamentoItemValorMedio AS CustoMedioFechamento,
                    fi.EST_FechamentoItemValorTotal AS ValorTotalEstoque
                FROM EST_FECHAMENTOITEM fi
                WHERE fi.EST_FechamentoData = :data_fechamento
                AND fi.EST_FechamentoItemQtd > 0
                AND fi.EST_FechamentoItemValorMedio > 0
                {filter_cf}
            ),
            CustoNFEntrada AS (
                SELECT
                    RTRIM(nfi.NFS_NotaFiscalItemProduto) AS Produto,
                    COUNT(DISTINCT nf.NFS_NotaFiscalNumero) AS QtdNFs,
                    SUM(nfi.NFS_NotaFiscalItemQuantidade) AS QtdTotalComprada,
                    CASE
                        WHEN SUM(nfi.NFS_NotaFiscalItemQuantidade) > 0
                        THEN SUM(nfi.NFS_NotaFiscalItemQuantidade * nfi.NFS_NotaFiscalItemCusto)
                            / SUM(nfi.NFS_NotaFiscalItemQuantidade)
                        ELSE 0
                    END AS CustoMedioPonderadoNF,
                    MIN(nfi.NFS_NotaFiscalItemCusto) AS MenorCustoNF,
                    MAX(nfi.NFS_NotaFiscalItemCusto) AS MaiorCustoNF
                FROM NFS_NOTAFISCALITEM nfi
                INNER JOIN NFS_NOTAFISCAL nf
                    ON nf.NFS_NotaFiscalFilial = nfi.NFS_NotaFiscalFilial
                    AND nf.NFS_NotaFiscalSeq = nfi.NFS_NotaFiscalSeq
                WHERE nf.NFS_NotaFiscalTipo = 'E'
                AND nf.NFS_NotaFiscalSituacao = 'C'
                AND nf.NFS_NotaFiscalDataEmissao >= :data_inicio_nf
                AND nfi.NFS_NotaFiscalItemCusto > 0
                AND nfi.NFS_NotaFiscalItemQuantidade > 0
                {filter_nf}
                GROUP BY RTRIM(nfi.NFS_NotaFiscalItemProduto)
            ),
            UltimaNF AS (
                SELECT
                    RTRIM(nfi.NFS_NotaFiscalItemProduto) AS Produto,
                    nfi.NFS_NotaFiscalItemCusto AS UltimoCustoNF,
                    nf.NFS_NotaFiscalDataEmissao AS DataUltimaNF,
                    ROW_NUMBER() OVER (
                        PARTITION BY RTRIM(nfi.NFS_NotaFiscalItemProduto)
                        ORDER BY nf.NFS_NotaFiscalDataEmissao DESC, nfi.NFS_NotaFiscalItemSeq DESC
                    ) AS rn
                FROM NFS_NOTAFISCALITEM nfi
                INNER JOIN NFS_NOTAFISCAL nf
                    ON nf.NFS_NotaFiscalFilial = nfi.NFS_NotaFiscalFilial
                    AND nf.NFS_NotaFiscalSeq = nfi.NFS_NotaFiscalSeq
                WHERE nf.NFS_NotaFiscalTipo = 'E'
                AND nf.NFS_NotaFiscalSituacao = 'C'
                AND nf.NFS_NotaFiscalDataEmissao >= :data_inicio_nf
                AND nfi.NFS_NotaFiscalItemCusto > 0
                AND nfi.NFS_NotaFiscalItemQuantidade > 0
                {filter_nf}
            )
            SELECT
                cf.Produto,
                RTRIM(p.PRO_Descricao) AS Descricao,
                CAST(cf.QtdEstoque AS DECIMAL(18,2)) AS QtdEstoque,
                CAST(cf.CustoMedioFechamento AS DECIMAL(18,4)) AS CustoMedio_Fech,
                CAST(cf.ValorTotalEstoque AS DECIMAL(18,2)) AS ValorTotal_Estoque,
                cnf.QtdNFs,
                CAST(cnf.QtdTotalComprada AS DECIMAL(18,2)) AS QtdComprada_NFs,
                CAST(cnf.CustoMedioPonderadoNF AS DECIMAL(18,4)) AS CustoMedioPond_NFs,
                CAST(cnf.MenorCustoNF AS DECIMAL(18,4)) AS MenorCusto_NF,
                CAST(cnf.MaiorCustoNF AS DECIMAL(18,4)) AS MaiorCusto_NF,
                CAST(ult.UltimoCustoNF AS DECIMAL(18,4)) AS UltimoCusto_NF,
                CONVERT(VARCHAR(10), ult.DataUltimaNF, 120) AS DataUltimaNF,
                CAST(
                    CASE
                        WHEN cnf.CustoMedioPonderadoNF > 0
                        THEN ((cf.CustoMedioFechamento - cnf.CustoMedioPonderadoNF)
                            / cnf.CustoMedioPonderadoNF) * 100
                    END AS DECIMAL(18,2)
                ) AS DifPct_FechVsNF,
                CASE
                    WHEN ABS(
                        CASE
                            WHEN cnf.CustoMedioPonderadoNF > 0
                            THEN ((cf.CustoMedioFechamento - cnf.CustoMedioPonderadoNF)
                                / cnf.CustoMedioPonderadoNF) * 100
                        END
                    ) > 10 THEN '*** VERIFICAR ***'
                    ELSE 'OK'
                END AS Status
            FROM CustoFechamento cf
            INNER JOIN PRO_PRODUTO p ON RTRIM(p.PRO_Codigo) = cf.Produto
            LEFT JOIN CustoNFEntrada cnf ON cnf.Produto = cf.Produto
            LEFT JOIN UltimaNF ult ON ult.Produto = cf.Produto AND ult.rn = 1
            WHERE cnf.Produto IS NOT NULL
            ORDER BY cf.ValorTotalEstoque DESC;
            """
        )
        if product_codes:
            query = query.bindparams(bindparam("product_codes", expanding=True))
        
        try:
            params: Dict[str, Any] = {
                "data_fechamento": data_fechamento,
                "data_inicio_nf": data_inicio_nf,
            }
            if product_codes:
                params["product_codes"] = product_codes
            rows = self.source_db.execute(query, params).mappings().all()
            logger.info(f"✓ Extraídos {len(rows)} registros de custo NF")
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"⚠ Erro ao extrair custo NF: {str(e)}")
            logger.warning("→ Retornando lista vazia (tabelas podem não existir ou credenciais inválidas)")
            return []

    def _load_custo_contabil(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        product_codes = sorted({row.get("CodigoProduto") for row in rows if row.get("CodigoProduto")})
        if product_codes:
            delete_query = text(
                "DELETE FROM product_cost_snapshot WHERE product_code IN :product_codes"
            ).bindparams(bindparam("product_codes", expanding=True))
            self.app_db.execute(delete_query, {"product_codes": product_codes})

        for row in rows:
            self.app_db.execute(
                text(
                    """
                    INSERT INTO product_cost_snapshot (
                        product_code,
                        filial,
                        data_fechamento,
                        data_custo,
                        custo_medio_real,
                        loaded_at
                    ) VALUES (
                        :product_code,
                        :filial,
                        :data_fechamento,
                        :data_custo,
                        :custo_medio_real,
                        NOW()
                    )
                    ON CONFLICT (product_code, filial, data_fechamento)
                    DO UPDATE SET
                        data_custo = EXCLUDED.data_custo,
                        custo_medio_real = EXCLUDED.custo_medio_real,
                        loaded_at = NOW();
                    """
                ),
                {
                    "product_code": row.get("CodigoProduto"),
                    "filial": row.get("Filial"),
                    "data_fechamento": row.get("MesFechamento"),
                    "data_custo": row.get("DataCusto"),
                    "custo_medio_real": row.get("CustoMedioReal"),
                },
            )

        return len(rows)

    def _load_custo_nf(self, rows: List[Dict[str, Any]], product_codes: List[str]) -> int:
        if not rows:
            return 0

        if product_codes:
            delete_query = text(
                "DELETE FROM product_nf_cost_analysis WHERE product_code IN :product_codes"
            ).bindparams(bindparam("product_codes", expanding=True))
            self.app_db.execute(delete_query, {"product_codes": product_codes})

        for row in rows:
            self.app_db.execute(
                text(
                    """
                    INSERT INTO product_nf_cost_analysis (
                        product_code,
                        descricao,
                        qtd_estoque,
                        custo_medio_fech,
                        valor_total_estoque,
                        qtd_nfs,
                        qtd_comprada_nfs,
                        custo_medio_pond_nfs,
                        menor_custo_nf,
                        maior_custo_nf,
                        ultimo_custo_nf,
                        data_ultima_nf,
                        dif_pct_fech_vs_nf,
                        status,
                        loaded_at
                    ) VALUES (
                        :product_code,
                        :descricao,
                        :qtd_estoque,
                        :custo_medio_fech,
                        :valor_total_estoque,
                        :qtd_nfs,
                        :qtd_comprada_nfs,
                        :custo_medio_pond_nfs,
                        :menor_custo_nf,
                        :maior_custo_nf,
                        :ultimo_custo_nf,
                        :data_ultima_nf,
                        :dif_pct_fech_vs_nf,
                        :status,
                        NOW()
                    )
                    ON CONFLICT (product_code)
                    DO UPDATE SET
                        descricao = EXCLUDED.descricao,
                        qtd_estoque = EXCLUDED.qtd_estoque,
                        custo_medio_fech = EXCLUDED.custo_medio_fech,
                        valor_total_estoque = EXCLUDED.valor_total_estoque,
                        qtd_nfs = EXCLUDED.qtd_nfs,
                        qtd_comprada_nfs = EXCLUDED.qtd_comprada_nfs,
                        custo_medio_pond_nfs = EXCLUDED.custo_medio_pond_nfs,
                        menor_custo_nf = EXCLUDED.menor_custo_nf,
                        maior_custo_nf = EXCLUDED.maior_custo_nf,
                        ultimo_custo_nf = EXCLUDED.ultimo_custo_nf,
                        data_ultima_nf = EXCLUDED.data_ultima_nf,
                        dif_pct_fech_vs_nf = EXCLUDED.dif_pct_fech_vs_nf,
                        status = EXCLUDED.status,
                        loaded_at = NOW();
                    """
                ),
                {
                    "product_code": row.get("Produto"),
                    "descricao": row.get("Descricao"),
                    "qtd_estoque": row.get("QtdEstoque"),
                    "custo_medio_fech": row.get("CustoMedio_Fech"),
                    "valor_total_estoque": row.get("ValorTotal_Estoque"),
                    "qtd_nfs": row.get("QtdNFs"),
                    "qtd_comprada_nfs": row.get("QtdComprada_NFs"),
                    "custo_medio_pond_nfs": row.get("CustoMedioPond_NFs"),
                    "menor_custo_nf": row.get("MenorCusto_NF"),
                    "maior_custo_nf": row.get("MaiorCusto_NF"),
                    "ultimo_custo_nf": row.get("UltimoCusto_NF"),
                    "data_ultima_nf": row.get("DataUltimaNF"),
                    "dif_pct_fech_vs_nf": row.get("DifPct_FechVsNF"),
                    "status": row.get("Status"),
                },
            )

        return len(rows)

    def _touch_product_status(self, rows: List[Dict[str, Any]], data_fechamento: str) -> None:
        now = datetime.now(timezone.utc)

        for row in rows:
            product_code = row.get("CodigoProduto") or row.get("Produto")
            if not product_code:
                continue

            self.app_db.execute(
                text(
                    """
                    INSERT INTO product_update_status (
                        product_code,
                        last_sync_at,
                        last_data_fechamento,
                        source
                    ) VALUES (
                        :product_code,
                        :last_sync_at,
                        :last_data_fechamento,
                        'sqlserver'
                    )
                    ON CONFLICT (product_code)
                    DO UPDATE SET
                        last_sync_at = EXCLUDED.last_sync_at,
                        last_data_fechamento = EXCLUDED.last_data_fechamento,
                        source = EXCLUDED.source;
                    """
                ),
                {
                    "product_code": product_code,
                    "last_sync_at": now,
                    "last_data_fechamento": data_fechamento,
                },
            )


def run_sync_job(source_db: Session, app_db: Session) -> Dict[str, Any]:
    """Executa o job com configuracoes padrao definidas no .env."""
    service = SyncService(source_db, app_db)
    return service.run_sync(
        data_fechamento=settings.default_data_fechamento,
        data_inicio_nf=settings.default_data_inicio_nf,
        product_codes=settings.sync_product_codes_list,
    )
