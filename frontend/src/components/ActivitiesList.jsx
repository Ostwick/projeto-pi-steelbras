import React from 'react';
import './ActivitiesList.css';

const ActivitiesList = ({ activities, isLoading }) => {
  if (isLoading) {
    return <div className="activities-list loading">Carregando atividades...</div>;
  }

  if (!activities || activities.length === 0) {
    return <div className="activities-list empty">Nenhuma atividade encontrada</div>;
  }

  const totalCost = activities.reduce((sum, activity) => sum + activity.total_cost, 0);

  return (
    <div className="activities-list">
      <div className="activities-header">
        <h4>Atividades e Processos</h4>
        <div className="activities-total">
          <span className="label">Custo Total de Atividades:</span>
          <span className="value">R$ {totalCost.toFixed(2)}</span>
        </div>
      </div>

      <div className="activities-table">
        <table>
          <thead>
            <tr>
              <th>Atividade</th>
              <th>Descrição</th>
              <th>Quantidade</th>
              <th>Custo Unitário</th>
              <th>Custo Total</th>
              <th>Última Atualização</th>
            </tr>
          </thead>
          <tbody>
            {activities.map((activity) => (
              <tr key={activity.activity_id}>
                <td className="activity-name">{activity.activity_name}</td>
                <td className="description">{activity.description || '-'}</td>
                <td className="quantity">{activity.quantity}</td>
                <td className="cost">R$ {activity.unit_cost.toFixed(2)}</td>
                <td className="cost highlight">R$ {activity.total_cost.toFixed(2)}</td>
                <td className="date">
                  {new Date(activity.last_update).toLocaleDateString('pt-BR')}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default ActivitiesList;
