/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_BACKEND_URL: string;
  readonly VITE_TRINKS_ESTABLISHMENT_ID: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
