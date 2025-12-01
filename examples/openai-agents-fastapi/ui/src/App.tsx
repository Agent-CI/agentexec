import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { Layout } from './components/Layout';
import { AgentListPage } from './pages/AgentListPage';
import { AgentDetailPage } from './pages/AgentDetailPage';

// Import the GitHub dark theme styles
import './styles/github-dark.css';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5000,
      retry: 1,
    },
  },
});

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route index element={<Navigate to="/agents" replace />} />
            <Route path="agents" element={<AgentListPage />} />
            <Route path="agents/:agentId" element={<AgentDetailPage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

export default App;
