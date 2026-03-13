from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_source_db

router = APIRouter(prefix="/api/queries", tags=["queries"])


class QueryRequest(BaseModel):
    query_name: str
    params: Dict[str, Any] = Field(default_factory=dict)


DEFAULT_DATA_FECHAMENTO = settings.default_data_fechamento
DEFAULT_DATA_INICIO_NF = settings.default_data_inicio_nf


QUERIES = {
    "cost_map": """
DECLARE @ProdutoCodigo CHAR(60) = :produto_codigo;
DECLARE @DataInicio DATE = :data_inicio;
DECLARE @DataFim DATE = :data_fim;
DECLARE @FilialCodigo DECIMAL(14,0) = :filial_codigo;

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

-- BOM Structure
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

-- OPs para PA (período) + SA (últimas 5)
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
""",
    "bom_cost_rollup": """
DECLARE @ProdutoCodigo CHAR(60) = :produto_codigo;
DECLARE @TipoCusto CHAR(1) = :tipo_custo;
DECLARE @QtdNotasMedia INT = :qtd_notas_media;
DECLARE @LimiteVariacaoPreco DECIMAL(5,2) = :limite_variacao_preco;

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
""",
    "custo_contabil": """WITH FechamentoMensal AS (
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
                        WHERE CUS_TipoValorCodigo = 1
                        )
                        SELECT
                        CodigoProduto,
                        Filial,
                        MesFechamento,
                        DataCusto,
                        CustoMedioReal
                        FROM FechamentoMensal
                        WHERE rn = 1
                        ORDER BY MesFechamento DESC, CodigoProduto;""",
    "custo_medio_estoque_vs_nfs": """;WITH CustoFechamento AS (
                                    SELECT
                                        RTRIM(fi.EST_FechamentoItemProduto) AS Produto,
                                        fi.EST_FechamentoItemQtd AS QtdEstoque,
                                        fi.EST_FechamentoItemValorMedio AS CustoMedioFechamento,
                                        fi.EST_FechamentoItemValorTotal AS ValorTotalEstoque
                                    FROM EST_FECHAMENTOITEM fi
                                    WHERE fi.EST_FechamentoData = :data_fechamento
                                    AND fi.EST_FechamentoItemQtd > 0
                                    AND fi.EST_FechamentoItemValorMedio > 0
                                    AND fi.FIL_Codigo = 8637511000120
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
                                            CASE WHEN cnf.CustoMedioPonderadoNF > 0
                                                THEN ((cf.CustoMedioFechamento - cnf.CustoMedioPonderadoNF)
                                                    / cnf.CustoMedioPonderadoNF) * 100
                                            END) > 10 THEN '*** VERIFICAR ***'
                                        ELSE 'OK'
                                    END AS Status
                                FROM CustoFechamento cf
                                INNER JOIN PRO_PRODUTO p ON RTRIM(p.PRO_Codigo) = cf.Produto
                                LEFT JOIN CustoNFEntrada cnf ON cnf.Produto = cf.Produto
                                LEFT JOIN UltimaNF ult ON ult.Produto = cf.Produto AND ult.rn = 1
                                WHERE cnf.Produto IS NOT NULL
                                ORDER BY cf.ValorTotalEstoque DESC;""",
}

@router.post("/execute")
async def execute_custom_query(
    payload: QueryRequest,
    db: Session = Depends(get_source_db),
):
    """
    Executa queries SQL customizadas.
    Use este endpoint para integrar suas queries SQL existentes.
    """
    query_name = payload.query_name
    params = payload.params or {}

    if query_name not in QUERIES:
        raise HTTPException(status_code=404, detail="Query não encontrada")

    if query_name == "custo_medio_estoque_vs_nfs":
        params.setdefault("data_fechamento", DEFAULT_DATA_FECHAMENTO)
        params.setdefault("data_inicio_nf", DEFAULT_DATA_INICIO_NF)
    
    try:
        sql_query = text(QUERIES[query_name])
        result = db.execute(sql_query, params)
        data = [dict(row) for row in result.mappings().all()]
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/custo-contabil/fechamentos")
async def get_available_fechamentos(db: Session = Depends(get_source_db)):
    """Retorna as datas de fechamento disponiveis para o frontend montar o seletor."""
    try:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT EOMONTH(CUS_ValorProdutoData) AS data_fechamento
                FROM CUS_VALORPRODUTO
                WHERE CUS_TipoValorCodigo = 1
                ORDER BY data_fechamento DESC;
                """
            )
        ).mappings().all()
        return {"fechamentos": [dict(row) for row in rows]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro ao listar fechamentos: {exc}")


@router.get("/health")
async def health_check():
    """Verifica se a API está funcionando"""
    return {"status": "ok", "message": "API de Análise de Custos está rodando"}
