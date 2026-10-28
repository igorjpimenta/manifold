// Hash-routed shell. Four views; no react-router needed — the routes are
// stable and tiny. `useHash()` watches `hashchange`; views mount/unmount on
// route change, which gives us free WS-subscription cleanup via React effects.

import { useEffect, useMemo, useState } from "react";
import { Layout } from "./components/Layout";
import { Home } from "./views/Home";
import { PipelinesList } from "./views/PipelinesList";
import { PipelineDetail } from "./views/PipelineDetail";
import { RunDetail } from "./views/RunDetail";
import { Search } from "./views/Search";
import { WsProvider } from "./api/ws";

function useHash(): string {
  const [hash, setHash] = useState<string>(() => window.location.hash || "#/");
  useEffect(() => {
    const onHash = () => setHash(window.location.hash || "#/");
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);
  return hash;
}

export default function App() {
  const hash = useHash();

  const view = useMemo(() => {
    const m = hash.match(/^#\/runs\/([0-9a-f-]+)$/i);
    if (m) return <RunDetail flowId={m[1]} />;
    const p = hash.match(/^#\/pipelines\/(.+)$/);
    if (p) return <PipelineDetail pipelineId={decodeURIComponent(p[1])} />;
    if (hash.startsWith("#/pipelines")) return <PipelinesList />;
    if (hash.startsWith("#/search")) return <Search />;
    return <Home />;
  }, [hash]);

  return (
    <WsProvider>
      <Layout>{view}</Layout>
    </WsProvider>
  );
}
