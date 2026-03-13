from pydantic import BaseModel
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
