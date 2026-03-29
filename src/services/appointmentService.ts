type ChatHistoryItem = {
  role: "user" | "assistant";
  content: string;
};

type KnowledgePayload = Record<string, unknown>;
type EvolutionStatusResponse = {
  status?: string;
  instance?: string;
  connected?: boolean;
  data?: Record<string, unknown> | null;
};

type EvolutionQrResponse = {
  status?: string;
  instance?: string;
  qr?: {
    hasQrImage?: boolean;
    qrDataUrl?: string | null;
    qrRaw?: string | null;
    pairingCode?: string | null;
    sourcePath?: string | null;
  };
};

type WhatsappConversation = {
  phone: string;
  name?: string;
  lastMessage?: string;
  lastRole?: string;
  updatedAt?: string;
  count?: number;
};

type WhatsappMessage = {
  role: "user" | "assistant";
  content: string;
  at?: string;
  senderName?: string;
};

type DbConversation = {
  phone: string;
  name?: string;
  lastMessage?: string;
  lastRole?: string;
  updatedAt?: string;
  count?: number;
};

type DbMessage = {
  id?: number;
  phone?: string;
  role: "user" | "assistant";
  content: string;
  senderName?: string;
  at?: string;
  source?: string;
};

type AppointmentAuditItem = {
  id?: number;
  eventType?: string;
  status?: string;
  establishmentId?: number;
  appointmentId?: number;
  confirmationCode?: string;
  clientPhone?: string;
  clientName?: string;
  serviceName?: string;
  professionalName?: string;
  date?: string;
  time?: string;
  requestPayload?: Record<string, unknown> | null;
  responsePayload?: Record<string, unknown> | null;
  errorMessage?: string;
  createdAt?: string;
};

type WebhookEventItem = {
  id?: number;
  event?: string;
  instanceName?: string;
  senderRaw?: string;
  senderNumber?: string;
  senderName?: string;
  messageId?: string;
  messageType?: string;
  messageText?: string;
  status?: string;
  reason?: string;
  details?: Record<string, unknown> | null;
  receivedAt?: string;
};

type AdminTenant = {
  id?: number;
  code: string;
  name: string;
  segment?: string;
  active?: boolean;
  defaultProvider?: string;
  establishmentId?: number | null;
  knowledge?: Record<string, unknown>;
  createdAt?: string;
  updatedAt?: string;
};

type AdminTenantIdentifier = {
  id?: number;
  kind: string;
  value: string;
  normalizedValue?: string;
  createdAt?: string;
  updatedAt?: string;
};

type AdminTenantProviderConfig = {
  id?: number;
  provider: string;
  enabled: boolean;
  config?: Record<string, unknown>;
  createdAt?: string;
  updatedAt?: string;
};

export class AppointmentService {
  private backendUrl: string;
  private establishmentId: string;

  constructor() {
    this.backendUrl = (import.meta.env.VITE_BACKEND_URL || "").trim();
    this.establishmentId = (import.meta.env.VITE_TRINKS_ESTABLISHMENT_ID || "").trim();
  }

  private getBackendEndpoint(path: string): string {
    if (!this.backendUrl) {
      throw new Error("VITE_BACKEND_URL nao configurado.");
    }
    return `${this.backendUrl}${path}`;
  }

  private async adminRequest<T>(path: string, token: string, init: RequestInit = {}): Promise<T> {
    const normalizedToken = String(token || "").trim();
    if (!normalizedToken) {
      throw new Error("Informe o token admin para acessar o painel.");
    }

    const headers = new Headers(init.headers || {});
    if (!headers.has("Content-Type") && init.method && init.method !== "GET") {
      headers.set("Content-Type", "application/json");
    }
    headers.set("x-admin-token", normalizedToken);

    const response = await fetch(this.getBackendEndpoint(path), {
      ...init,
      headers,
    });

    if (!response.ok) {
      let message = `Falha na operacao admin (${response.status}).`;
      try {
        const payload = (await response.json()) as { message?: string };
        if (payload?.message) {
          message = payload.message;
        }
      } catch {
        // Keep fallback message.
      }
      throw new Error(message);
    }

    return (await response.json()) as T;
  }

  async sendMessage(message: string, history: ChatHistoryItem[] = []) {
    if (!this.establishmentId) {
      throw new Error("VITE_TRINKS_ESTABLISHMENT_ID nao configurado.");
    }

    const response = await fetch(this.getBackendEndpoint("/api/chat"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        establishmentId: Number(this.establishmentId),
        message,
        history,
      }),
    });

    if (!response.ok) {
      let errorMessage = `Falha ao processar conversa (${response.status}).`;

      try {
        const errorData = (await response.json()) as { message?: string; details?: string };
        if (errorData?.message) {
          errorMessage = errorData.message;
        }
      } catch {
        // Ignore JSON parsing failures and keep the fallback message.
      }

      throw new Error(errorMessage);
    }

    const data = await response.json();
    return data.text as string;
  }

  async getKnowledge() {
    const response = await fetch(this.getBackendEndpoint("/api/knowledge"), {
      method: "GET",
      headers: { "Content-Type": "application/json" },
    });

    if (!response.ok) {
      throw new Error(`Falha ao carregar base de conhecimento (${response.status}).`);
    }

    const data = (await response.json()) as { knowledge?: KnowledgePayload };
    return data.knowledge || {};
  }

  async saveKnowledge(knowledge: KnowledgePayload) {
    const response = await fetch(this.getBackendEndpoint("/api/knowledge"), {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ knowledge }),
    });

    if (!response.ok) {
      let message = `Falha ao salvar base de conhecimento (${response.status}).`;
      try {
        const payload = (await response.json()) as { message?: string };
        if (payload?.message) {
          message = payload.message;
        }
      } catch {
        // Keep fallback message.
      }
      throw new Error(message);
    }

    const data = (await response.json()) as { knowledge?: KnowledgePayload };
    return data.knowledge || {};
  }

  async createEvolutionInstance(instance: string) {
    const response = await fetch(this.getBackendEndpoint("/api/evolution/instance/create"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ instance }),
    });

    if (!response.ok) {
      let message = `Falha ao criar instancia (${response.status}).`;
      try {
        const payload = (await response.json()) as { message?: string };
        if (payload?.message) {
          message = payload.message;
        }
      } catch {
        // Keep fallback message.
      }
      throw new Error(message);
    }

    return response.json();
  }

  async disconnectEvolutionInstance(instance: string) {
    const response = await fetch(
      this.getBackendEndpoint(`/api/evolution/instance/disconnect?instance=${encodeURIComponent(instance)}`),
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      },
    );

    if (!response.ok) {
      let message = `Falha ao desconectar (${response.status}).`;
      try {
        const payload = (await response.json()) as { message?: string };
        if (payload?.message) {
          message = payload.message;
        }
      } catch {
        // Keep fallback message.
      }
      throw new Error(message);
    }

    return (await response.json()) as { success: boolean; message?: string };
  }
  async getEvolutionQr(instance: string) {
    const response = await fetch(
      this.getBackendEndpoint(`/api/evolution/instance/qr?instance=${encodeURIComponent(instance)}`),
      {
        method: "GET",
        headers: { "Content-Type": "application/json" },
      },
    );

    if (!response.ok) {
      let message = `Falha ao buscar QR (${response.status}).`;
      try {
        const payload = (await response.json()) as { message?: string };
        if (payload?.message) {
          message = payload.message;
        }
      } catch {
        // Keep fallback message.
      }
      throw new Error(message);
    }

    return (await response.json()) as EvolutionQrResponse;
  }

  async getEvolutionStatus(instance: string) {
    const response = await fetch(
      this.getBackendEndpoint(`/api/evolution/instance/status?instance=${encodeURIComponent(instance)}`),
      {
        method: "GET",
        headers: { "Content-Type": "application/json" },
      },
    );

    if (!response.ok) {
      let message = `Falha ao consultar status (${response.status}).`;
      try {
        const payload = (await response.json()) as { message?: string };
        if (payload?.message) {
          message = payload.message;
        }
      } catch {
        // Keep fallback message.
      }
      throw new Error(message);
    }

    return (await response.json()) as EvolutionStatusResponse;
  }

  async getWhatsappConversations() {
    const response = await fetch(this.getBackendEndpoint("/api/whatsapp/inbox"), {
      method: "GET",
      headers: { "Content-Type": "application/json" },
    });

    if (!response.ok) {
      throw new Error(`Falha ao carregar inbox (${response.status}).`);
    }

    const data = (await response.json()) as { conversations?: WhatsappConversation[] };
    return Array.isArray(data.conversations) ? data.conversations : [];
  }

  async getWhatsappMessages(phone: string) {
    const response = await fetch(
      this.getBackendEndpoint(`/api/whatsapp/messages?phone=${encodeURIComponent(phone)}`),
      {
        method: "GET",
        headers: { "Content-Type": "application/json" },
      },
    );

    if (!response.ok) {
      throw new Error(`Falha ao carregar mensagens (${response.status}).`);
    }

    const data = (await response.json()) as { messages?: WhatsappMessage[] };
    return Array.isArray(data.messages) ? data.messages : [];
  }

  async sendWhatsappMessage(to: string, text: string) {
    const response = await fetch(this.getBackendEndpoint("/api/evolution/send-text"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ to, text }),
    });

    if (!response.ok) {
      let message = `Falha ao enviar mensagem (${response.status}).`;
      try {
        const payload = (await response.json()) as { message?: string };
        if (payload?.message) {
          message = payload.message;
        }
      } catch {
        // Keep fallback message.
      }
      throw new Error(message);
    }

    return response.json();
  }

  async getDbConversations(limit = 100) {
    const response = await fetch(
      this.getBackendEndpoint(`/api/db/conversations?limit=${encodeURIComponent(String(limit))}`),
      {
        method: "GET",
        headers: { "Content-Type": "application/json" },
      },
    );

    if (!response.ok) {
      throw new Error(`Falha ao carregar conversas do banco (${response.status}).`);
    }

    const data = (await response.json()) as { data?: DbConversation[] };
    return Array.isArray(data.data) ? data.data : [];
  }

  async getDbMessages(phone: string, limit = 300) {
    const response = await fetch(
      this.getBackendEndpoint(
        `/api/db/messages?phone=${encodeURIComponent(phone)}&limit=${encodeURIComponent(String(limit))}`,
      ),
      {
        method: "GET",
        headers: { "Content-Type": "application/json" },
      },
    );

    if (!response.ok) {
      throw new Error(`Falha ao carregar mensagens do banco (${response.status}).`);
    }

    const data = (await response.json()) as { messages?: DbMessage[] };
    return Array.isArray(data.messages) ? data.messages : [];
  }

  async getAppointmentsAudit(options: { phone?: string; status?: string; limit?: number } = {}) {
    const params = new URLSearchParams();
    params.set("limit", String(options.limit ?? 300));
    if (options.phone) {
      params.set("phone", options.phone);
    }
    if (options.status) {
      params.set("status", options.status);
    }

    const response = await fetch(
      this.getBackendEndpoint(`/api/db/appointments-audit?${params.toString()}`),
      {
        method: "GET",
        headers: { "Content-Type": "application/json" },
      },
    );

    if (!response.ok) {
      throw new Error(`Falha ao carregar auditoria (${response.status}).`);
    }

    const data = (await response.json()) as { data?: AppointmentAuditItem[] };
    return Array.isArray(data.data) ? data.data : [];
  }

  async getWebhookEvents(options: { phone?: string; status?: string; reason?: string; limit?: number } = {}) {
    const params = new URLSearchParams();
    params.set("limit", String(options.limit ?? 300));
    if (options.phone) {
      params.set("phone", options.phone);
    }
    if (options.status) {
      params.set("status", options.status);
    }
    if (options.reason) {
      params.set("reason", options.reason);
    }

    const response = await fetch(
      this.getBackendEndpoint(`/api/db/webhook-events?${params.toString()}`),
      {
        method: "GET",
        headers: { "Content-Type": "application/json" },
      },
    );

    if (!response.ok) {
      throw new Error(`Falha ao carregar eventos de webhook (${response.status}).`);
    }

    const data = (await response.json()) as { data?: WebhookEventItem[] };
    return Array.isArray(data.data) ? data.data : [];
  }

  async getAdminTenants(adminToken: string, options: { withDetails?: boolean; includeInactive?: boolean } = {}) {
    const params = new URLSearchParams();
    if (options.withDetails) {
      params.set("withDetails", "true");
    }
    if (options.includeInactive) {
      params.set("includeInactive", "true");
    }
    const suffix = params.toString() ? `?${params.toString()}` : "";
    const data = await this.adminRequest<{ data?: AdminTenant[] }>(`/api/admin/tenants${suffix}`, adminToken, {
      method: "GET",
    });
    return Array.isArray(data.data) ? data.data : [];
  }

  async createAdminTenant(
    adminToken: string,
    payload: {
      code?: string;
      name: string;
      segment?: string;
      defaultProvider?: string;
      establishmentId?: number;
      active?: boolean;
    },
  ) {
    return this.adminRequest<{
      tenant?: AdminTenant;
      identifiers?: AdminTenantIdentifier[];
      providerConfigs?: AdminTenantProviderConfig[];
    }>("/api/admin/tenants", adminToken, {
      method: "POST",
      body: JSON.stringify(payload),
    });
  }

  async getAdminTenant(adminToken: string, code: string) {
    return this.adminRequest<{
      tenant?: AdminTenant;
      identifiers?: AdminTenantIdentifier[];
      providerConfigs?: AdminTenantProviderConfig[];
    }>(`/api/admin/tenants/${encodeURIComponent(code)}`, adminToken, {
      method: "GET",
    });
  }

  async updateAdminTenant(
    adminToken: string,
    code: string,
    payload: {
      name?: string;
      segment?: string;
      defaultProvider?: string;
      establishmentId?: number;
      active?: boolean;
    },
  ) {
    return this.adminRequest<{
      tenant?: AdminTenant;
      identifiers?: AdminTenantIdentifier[];
      providerConfigs?: AdminTenantProviderConfig[];
    }>(`/api/admin/tenants/${encodeURIComponent(code)}`, adminToken, {
      method: "PUT",
      body: JSON.stringify(payload),
    });
  }

  async resolveAdminTenant(adminToken: string, kind: string, value: string) {
    const params = new URLSearchParams({ kind, value });
    return this.adminRequest<{ found?: boolean; tenant?: AdminTenant | null }>(
      `/api/admin/tenants/resolve?${params.toString()}`,
      adminToken,
      { method: "GET" },
    );
  }

  async addAdminTenantIdentifier(adminToken: string, code: string, kind: string, value: string) {
    return this.adminRequest<{ identifiers?: AdminTenantIdentifier[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/identifiers`,
      adminToken,
      {
        method: "POST",
        body: JSON.stringify({ kind, value }),
      },
    );
  }

  async setAdminTenantProviderConfig(
    adminToken: string,
    code: string,
    provider: string,
    payload: { enabled?: boolean; config?: Record<string, unknown> },
  ) {
    return this.adminRequest<{ providerConfigs?: AdminTenantProviderConfig[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/providers/${encodeURIComponent(provider)}`,
      adminToken,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
    );
  }
}

