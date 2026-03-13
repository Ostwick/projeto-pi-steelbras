-- ============================================================================
-- PARÂMETROS DE ENTRADA
-- ============================================================================
DECLARE @ProdutoCodigo CHAR(60) = '00020011';  -- << ALTERE AQUI O CÓDIGO DO PRODUTO
DECLARE @TipoCusto CHAR(1) = 'U';              -- 'U' = Último Custo, 'M' = Média
DECLARE @QtdNotasMedia INT = 100;                 -- Quantas notas considerar na média
DECLARE @LimiteVariacaoPreco DECIMAL(5,2) = 15.00;  -- % limite para alertar variação de preço

-- ============================================================================
-- TABELA TEMPORÁRIA PARA EXPLOSÃO RECURSIVA DA BOM
-- ============================================================================
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
    TipoProduto VARCHAR(20),        -- 'ACABADO', 'SEMI-ACABADO', 'MATERIA-PRIMA'
    TemEstrutura CHAR(1),           -- 'S' ou 'N'
    CaminhoHierarquia VARCHAR(MAX), -- Para visualização da árvore
    NumeroHierarquia VARCHAR(50)    -- Numeração tipo 001.001.001
);

-- ============================================================================
-- CTE RECURSIVA PARA EXPLODIR A ESTRUTURA
-- ============================================================================
;WITH BomRecursiva AS (
    -- Nível 0: Produto raiz (produto que queremos custear)
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

    -- Níveis subsequentes: componentes da estrutura
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
        AND e.ENG_EstruturaTipo = 'P'  -- Tipo Produção
    INNER JOIN ENG_ESTRUTURAITEM ei 
        ON ei.ENG_EstruturaProduto = e.ENG_EstruturaProduto
        AND ei.ENG_EstruturaNumero = e.ENG_EstruturaNumero
    INNER JOIN PRO_PRODUTO p 
        ON p.PRO_Codigo = ei.ENG_EstruturaItemProduto
    WHERE r.Nivel < 10  -- Limite de segurança para evitar loops infinitos
)
INSERT INTO #BomExplodida
SELECT * FROM BomRecursiva;

-- ============================================================================
-- BUSCAR CUSTOS DAS MATÉRIAS-PRIMAS (NOTAS DE ENTRADA)
-- ============================================================================
IF OBJECT_ID('tempdb..#CustosMP') IS NOT NULL DROP TABLE #CustosMP;

CREATE TABLE #CustosMP (
    ProdutoCodigo CHAR(60),
    UltimoCusto DECIMAL(19,9),
    UltimaNF VARCHAR(50),                -- Número da NF de onde veio o último custo
    PenultimoCusto DECIMAL(19,9),
    VariacaoPreco DECIMAL(10,2),         -- % de variação entre última e penúltima
    CustoMedio DECIMAL(19,9),
    CustoContabil DECIMAL(17,2),         -- Custo médio do último fechamento contábil
    RefFechamento DECIMAL(18,0),         -- Referência (AAAAMM) do fechamento usado
    CustoFinal DECIMAL(19,9),            -- Custo a ser usado (nota ou contábil)
    OrigemCusto VARCHAR(20),             -- 'NOTA', 'CONTABIL' ou 'SEM CUSTO'
    DataUltimaCompra DATETIME,
    QtdNotasEncontradas INT
);

-- Calcular custos para cada matéria-prima única
INSERT INTO #CustosMP
SELECT 
    mp.ProdutoFilho,
    -- Último custo (nota mais recente)
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
    -- Número da NF que originou o último custo
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
    -- Penúltimo custo (segunda nota mais recente)
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
    -- Variação de preço (calculado depois)
    NULL AS VariacaoPreco,
    -- Custo médio das últimas N notas
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
    -- Custo contábil: último fechamento de custo do produto
    (SELECT TOP 1 fc.EST_FechamentoCstCustoMedio
     FROM EST_FECHAMENTOCUSTO fc
     WHERE fc.EST_FechamentoCstProduto = mp.ProdutoFilho
       AND fc.EST_FechamentoCstCustoMedio > 0
     ORDER BY fc.EST_FechamentoCstReferencia DESC
    ) AS CustoContabil,
    -- Referência do fechamento contábil usado
    (SELECT TOP 1 fc.EST_FechamentoCstReferencia
     FROM EST_FECHAMENTOCUSTO fc
     WHERE fc.EST_FechamentoCstProduto = mp.ProdutoFilho
       AND fc.EST_FechamentoCstCustoMedio > 0
     ORDER BY fc.EST_FechamentoCstReferencia DESC
    ) AS RefFechamento,
    -- Custo final e origem serão calculados depois
    NULL AS CustoFinal,
    NULL AS OrigemCusto,
    -- Data última compra
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
    -- Quantidade de notas encontradas
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
    WHERE TemEstrutura = 'N'  -- Somente itens folha (sem estrutura própria)
) mp;

-- Calcular variação de preço entre última e penúltima nota
UPDATE #CustosMP
SET VariacaoPreco = CASE 
        WHEN PenultimoCusto IS NOT NULL AND PenultimoCusto > 0 
        THEN ((UltimoCusto - PenultimoCusto) / PenultimoCusto) * 100
        ELSE NULL 
    END;

-- Definir custo final e origem (prioriza nota, senão usa fechamento contábil)
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

-- ============================================================================
-- RESULTADO 1: DETALHAMENTO DA ESTRUTURA COM CUSTOS
-- ============================================================================
SELECT 
    '=== DETALHAMENTO DA ESTRUTURA ===' AS Secao;

SELECT 
    b.NumeroHierarquia AS Estrutura,
    b.Nivel,
    CASE b.Nivel
        WHEN 0 THEN RTRIM(b.ProdutoFilho)
        ELSE REPLICATE('│   ', b.Nivel - 1) + '└── ' + RTRIM(b.ProdutoFilho)
    END AS Hierarquia,
    b.DescricaoFilho AS Descricao,
    b.TipoProduto,
    b.QuantidadeComPerda AS Quantidade,
    b.UnidadeMedida AS UN,
    CASE 
        WHEN b.TemEstrutura = 'N' THEN 
            CASE @TipoCusto 
                WHEN 'U' THEN c.CustoFinal 
                ELSE ISNULL(c.CustoMedio, c.CustoFinal) 
            END
        ELSE NULL 
    END AS CustoUnitario,
    CASE 
        WHEN b.TemEstrutura = 'N' THEN 
            b.QuantidadeComPerda * CASE @TipoCusto 
                WHEN 'U' THEN ISNULL(c.CustoFinal, 0) 
                ELSE ISNULL(c.CustoMedio, ISNULL(c.CustoFinal, 0)) 
            END
        ELSE NULL 
    END AS CustoTotal,
    c.OrigemCusto,
    c.VariacaoPreco,
    CASE 
        WHEN c.VariacaoPreco IS NOT NULL AND ABS(c.VariacaoPreco) > @LimiteVariacaoPreco 
        THEN '⚠️ VAR ' + CAST(CAST(c.VariacaoPreco AS DECIMAL(10,1)) AS VARCHAR) + '%'
        WHEN b.TemEstrutura = 'N' AND c.CustoFinal IS NULL THEN '❌ SEM CUSTO'
        WHEN c.OrigemCusto = 'CONTABIL' THEN '📋 CONTABIL (Ref: ' + CAST(c.RefFechamento AS VARCHAR) + ')'
        ELSE '' 
    END AS Alerta,
    CASE WHEN @TipoCusto = 'U' THEN c.UltimaNF ELSE NULL END AS NotaFiscal,
    c.DataUltimaCompra,
    c.QtdNotasEncontradas
FROM #BomExplodida b
LEFT JOIN #CustosMP c ON c.ProdutoCodigo = b.ProdutoFilho
ORDER BY b.NumeroHierarquia;

-- ============================================================================
-- RESULTADO 2: RESUMO POR MATÉRIA-PRIMA (ITENS FOLHA)
-- ============================================================================
SELECT 
    '=== RESUMO POR MATÉRIA-PRIMA ===' AS Secao;

SELECT 
    RTRIM(b.ProdutoFilho) AS Codigo,
    b.DescricaoFilho AS Descricao,
    b.TipoProduto,
    SUM(b.QuantidadeComPerda) AS QuantidadeTotal,
    b.UnidadeMedida AS UN,
    CASE @TipoCusto 
        WHEN 'U' THEN c.CustoFinal 
        ELSE ISNULL(c.CustoMedio, c.CustoFinal) 
    END AS CustoUnitario,
    SUM(b.QuantidadeComPerda) * CASE @TipoCusto 
        WHEN 'U' THEN ISNULL(c.CustoFinal, 0) 
        ELSE ISNULL(c.CustoMedio, ISNULL(c.CustoFinal, 0)) 
    END AS CustoTotalItem,
    c.OrigemCusto,
    c.VariacaoPreco,
    CASE 
        WHEN c.VariacaoPreco IS NOT NULL AND ABS(c.VariacaoPreco) > @LimiteVariacaoPreco 
        THEN '⚠️ ALERTA: Variação de ' + CAST(CAST(c.VariacaoPreco AS DECIMAL(10,1)) AS VARCHAR) + '% entre última e penúltima compra'
        ELSE ''
    END AS AlertaVariacao,
    CASE WHEN @TipoCusto = 'U' THEN c.UltimaNF ELSE NULL END AS NotaFiscal,
    c.DataUltimaCompra,
    CASE 
        WHEN c.CustoFinal IS NULL THEN '❌ SEM CUSTO'
        WHEN c.OrigemCusto = 'CONTABIL' THEN '📋 CONTABIL (Ref: ' + CAST(c.RefFechamento AS VARCHAR) + ')'
        WHEN c.QtdNotasEncontradas < @QtdNotasMedia THEN '⚠️ POUCAS NOTAS'
        ELSE '✅ OK' 
    END AS Status
FROM #BomExplodida b
LEFT JOIN #CustosMP c ON c.ProdutoCodigo = b.ProdutoFilho
WHERE b.TemEstrutura = 'N'  -- Somente matérias-primas (itens folha)
GROUP BY 
    b.ProdutoFilho, 
    b.DescricaoFilho, 
    b.TipoProduto,
    b.UnidadeMedida,
    c.CustoFinal,
    c.CustoMedio,
    c.OrigemCusto,
    c.VariacaoPreco,
    c.UltimaNF,
    c.RefFechamento,
    c.DataUltimaCompra,
    c.QtdNotasEncontradas
ORDER BY CustoTotalItem DESC;

-- ============================================================================
-- RESULTADO 3: CUSTO TOTAL ESTIMADO
-- ============================================================================
SELECT 
    '=== CUSTO TOTAL ESTIMADO ===' AS Secao;

SELECT 
    @ProdutoCodigo AS ProdutoCodigo,
    (SELECT PRO_Descricao FROM PRO_PRODUTO WHERE PRO_Codigo = @ProdutoCodigo) AS ProdutoDescricao,
    @TipoCusto AS TipoCustoUsado,
    CASE @TipoCusto WHEN 'U' THEN 'Último Custo' ELSE 'Média Últimas ' + CAST(@QtdNotasMedia AS VARCHAR) + ' Notas' END AS MetodoCusteio,
    SUM(
        b.QuantidadeComPerda * CASE @TipoCusto 
            WHEN 'U' THEN ISNULL(c.CustoFinal, 0) 
            ELSE ISNULL(c.CustoMedio, ISNULL(c.CustoFinal, 0)) 
        END
    ) AS CustoTotalMateriaPrima,
    COUNT(DISTINCT b.ProdutoFilho) AS QtdMateriaPrimasDistintas,
    SUM(CASE WHEN c.CustoFinal IS NULL THEN 1 ELSE 0 END) AS QtdItensSemCusto,
    SUM(CASE WHEN c.OrigemCusto = 'CONTABIL' THEN 1 ELSE 0 END) AS QtdItensComCustoContabil,
    SUM(CASE WHEN c.VariacaoPreco IS NOT NULL AND ABS(c.VariacaoPreco) > @LimiteVariacaoPreco THEN 1 ELSE 0 END) AS QtdItensComVariacaoAlta,
    GETDATE() AS DataSimulacao
FROM #BomExplodida b
LEFT JOIN #CustosMP c ON c.ProdutoCodigo = b.ProdutoFilho
WHERE b.TemEstrutura = 'N';

-- ============================================================================
-- RESULTADO 4: ALERTAS E ITENS COM PROBLEMA
-- ============================================================================
SELECT 
    '=== ALERTAS E PROBLEMAS ===' AS Secao;

-- 4.1 Itens com variação de preço acima do limite
SELECT 
    'VARIAÇÃO DE PREÇO' AS TipoAlerta,
    RTRIM(c.ProdutoCodigo) AS Codigo,
    (SELECT PRO_Descricao FROM PRO_PRODUTO WHERE PRO_Codigo = c.ProdutoCodigo) AS Descricao,
    c.PenultimoCusto AS CustoAnterior,
    c.UltimoCusto AS CustoAtual,
    c.VariacaoPreco AS VariacaoPercentual,
    '⚠️ Variação de ' + CAST(CAST(c.VariacaoPreco AS DECIMAL(10,1)) AS VARCHAR) + '% (limite: ' + CAST(@LimiteVariacaoPreco AS VARCHAR) + '%)' AS Alerta
FROM #CustosMP c
WHERE c.VariacaoPreco IS NOT NULL 
  AND ABS(c.VariacaoPreco) > @LimiteVariacaoPreco

UNION ALL

-- 4.2 Itens usando custo do fechamento contábil (sem nota de entrada)
SELECT 
    'CUSTO CONTABIL' AS TipoAlerta,
    RTRIM(c.ProdutoCodigo) AS Codigo,
    (SELECT PRO_Descricao FROM PRO_PRODUTO WHERE PRO_Codigo = c.ProdutoCodigo) AS Descricao,
    NULL AS CustoAnterior,
    c.CustoContabil AS CustoAtual,
    NULL AS VariacaoPercentual,
    '📋 Sem notas - custo contábil (Ref: ' + CAST(c.RefFechamento AS VARCHAR) + '): R$ ' + CAST(CAST(c.CustoContabil AS DECIMAL(15,2)) AS VARCHAR) AS Alerta
FROM #CustosMP c
WHERE c.OrigemCusto = 'CONTABIL'

UNION ALL

-- 4.3 Itens totalmente sem custo
SELECT 
    'SEM CUSTO' AS TipoAlerta,
    RTRIM(c.ProdutoCodigo) AS Codigo,
    (SELECT PRO_Descricao FROM PRO_PRODUTO WHERE PRO_Codigo = c.ProdutoCodigo) AS Descricao,
    NULL AS CustoAnterior,
    NULL AS CustoAtual,
    NULL AS VariacaoPercentual,
    '❌ Nenhum custo encontrado (nota ou cadastro)' AS Alerta
FROM #CustosMP c
WHERE c.OrigemCusto = 'SEM CUSTO'

ORDER BY TipoAlerta, Codigo;

-- ============================================================================
-- LIMPEZA
-- ============================================================================
DROP TABLE #BomExplodida;
DROP TABLE #CustosMP;
