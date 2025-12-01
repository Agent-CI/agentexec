import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { Layout } from './components/Layout';
import { AgentListPage } from './pages/AgentListPage';
import { AgentDetailPage } from './pages/AgentDetailPage';

// Import the GitHub dark theme styles
import 'agentexec-ui/styles';

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/agents" replace />} />
          <Route path="agents" element={<AgentListPage />} />
          <Route path="agents/:agentId" element={<AgentDetailPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App;
