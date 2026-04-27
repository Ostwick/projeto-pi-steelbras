from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


# Schema para componente na árvore de composição
class ComponentCost(BaseModel):
    component_id: str
    component_name: str
    quantity: float
    unit_cost: float
    total_cost: float
    average_cost: float
    last_costs: List[dict]  # Últimos custos históricos
    
    class Config:
        from_attributes = True


# Schema para produto com árvore de composição
class ProductComposition(BaseModel):
    product_id: str
    product_name: str
    product_code: str
    total_cost: float
    average_cost: float
    components: List[ComponentCost]
    
    class Config:
        from_attributes = True


# Schema para atividade/processo com custo
class ActivityCost(BaseModel):
    activity_id: int
    activity_name: str
    description: Optional[str]
    unit_cost: float
    total_cost: float
    quantity: float
    last_update: datetime
    
    class Config:
        from_attributes = True


# Schema para lista de produtos
class ProductSummary(BaseModel):
    product_id: str
    product_name: str
    product_code: str
    current_cost: float
    last_update: datetime
    
    class Config:
        from_attributes = True


# Schema para resposta de busca de produtos
class ProductSearchResponse(BaseModel):
    products: List[ProductSummary]
    total: int
    
    class Config:
        from_attributes = True


class AlertRecipient(BaseModel):
    email: str
    name: Optional[str] = None


class AlertThresholds(BaseModel):
    invoice_increase_pct: float = 5.0
    avg_cost_increase_pct: float = 3.0
    avg_cost_vs_last_invoice_pct: float = 10.0


class SMTPConfig(BaseModel):
    server: Optional[str] = None
    port: int = 465
    user: Optional[str] = None
    has_password: bool = False


class SMTPConfigUpdate(BaseModel):
    server: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None


class AlertSettingsResponse(BaseModel):
    smtp: SMTPConfig
    recipients: List[AlertRecipient] = Field(default_factory=list)
    thresholds: dict = Field(default_factory=dict)
    split_finished_goods: bool = True


class AlertSettingsUpdate(BaseModel):
    smtp: Optional[SMTPConfigUpdate] = None
    recipients: Optional[List[AlertRecipient]] = None
    thresholds: Optional[AlertThresholds] = None
    split_finished_goods: Optional[bool] = None


class CostVarianceItem(BaseModel):
    product_code: str
    product_name: str
    last_invoice_cost: float
    average_cost: float
    variance_pct: float
    is_finished_good: bool = False


class RuleHitItem(BaseModel):
    product_code: str
    product_name: str
    variation_pct: float
    current_value: float
    reference_value: float
    latest_invoice_number: Optional[str] = None
    previous_invoice_number: Optional[str] = None
    is_finished_good: bool = False


class AlertPreviewResponse(BaseModel):
    has_new_invoice: bool
    items: List[CostVarianceItem]
    message: Optional[str] = None
    rule_hits: dict = Field(default_factory=dict)
    generated_at: Optional[datetime] = None


class ProductAlertItem(BaseModel):
    product_code: str
    tipo_alerta: str
    descricao: Optional[str] = None
    custo_atual: Optional[float] = None
    variacao_percentual: Optional[float] = None
    mensagem_alerta: Optional[str] = None
    severidade: Optional[str] = None
    data_referencia: Optional[datetime] = None
    sent: bool = False


class ProductAlertsResponse(BaseModel):
    product_code: str
    data_referencia: Optional[str] = None
    alerts: List[ProductAlertItem] = Field(default_factory=list)
    pending_count: int = 0


class AlertDispatchResponse(BaseModel):
    product_code: str
    sent_count: int
    skipped_count: int
    failed_recipients: List[str] = Field(default_factory=list)
    message: str
