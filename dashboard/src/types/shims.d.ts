/// <reference types="vite/client" />
// cytoscape-dagre ships a UMD bundle but no types. We don't need a typed
// export — cytoscape.use() accepts a plug-in function and that's all we call.
declare module "cytoscape-dagre";
