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

type AdminTenantUser = {
  id?: number;
  tenantId?: number;
  tenantCode?: string;
  tenantName?: string;
  username: string;
  displayName?: string;
  active?: boolean;
  lastLoginAt?: string;
  createdAt?: string;
  updatedAt?: string;
};

type AdminPrincipal = {
  role: "superadmin" | "tenant";
  tokenType?: string;
  tenantCode?: string;
  tenantName?: string;
  username?: string;
  displayName?: string;
  expiresAt?: string;
};

type CrmSettings = {
  crmReturnEnabled?: boolean;
  crmMode?: "beta" | "manual" | "automatic";
  bookingMaxDaysAhead?: number;
  messageSendingWindowStart?: string;
  messageSendingWindowEnd?: string;
  messageDailyLimit?: number;
  stopFlowOnAnyFutureBooking?: boolean;
  maxSteps?: number;
  humanHandoffEnabled?: boolean;
  humanHandoffClientNumber?: string;
  humanHandoffInternalNumber?: string;
  humanHandoffMessageTemplate?: string;
  humanHandoffSendInternalSummary?: boolean;
  humanHandoffPauseAi?: boolean;
  opportunityTrackingEnabled?: boolean;
  allowOnlyWhitelistedPhonesInBeta?: boolean;
  betaTestPhones?: string[];
};

type CrmServiceRule = {
  id?: number;
  serviceKey: string;
  serviceName: string;
  categoryKey?: string;
  categoryName?: string;
  active?: boolean;
  returnDays?: number | null;
  useDefaultFlow?: boolean;
  step1DelayDays?: number | null;
  step1MessageTemplate?: string;
  step2DelayDays?: number | null;
  step2MessageTemplate?: string;
  step3DelayDays?: number | null;
  step3MessageTemplate?: string;
  priority?: "low" | "medium" | "high";
  notes?: string;
};

type CrmCategoryRule = {
  id?: number;
  categoryKey: string;
  categoryName: string;
  opportunityTrackingEnabled?: boolean;
  opportunityDaysWithoutReturn?: number | null;
  opportunityPriority?: "low" | "medium" | "high";
  allowManualCampaign?: boolean;
  suggestedMessageTemplate?: string;
  notes?: string;
};

type CrmClientBlock = {
  id?: number;
  clientId?: number | null;
  clientName?: string;
  phone: string;
  isBlocked?: boolean;
  blockReason?: string;
  blockNotes?: string;
  blockedAt?: string;
  blockedBy?: string;
  createdAt?: string;
  updatedAt?: string;
};

type CrmFlowItem = {
  id?: number;
  clientId?: number | null;
  clientName?: string;
  phone?: string;
  originServiceName?: string;
  originCategoryName?: string;
  lastVisitAt?: string;
  lastProfessionalId?: number | null;
  lastProfessionalName?: string;
  lastProfessionalActive?: boolean | null;
  flowStatus?: string;
  currentStep?: number;
  enteredFlowAt?: string;
  lastMessageSentAt?: string;
  nextScheduledSendAt?: string;
  stopReason?: string;
  convertedAppointmentId?: number | null;
  convertedAt?: string;
};

type CrmOpportunityItem = {
  id?: number;
  clientId?: number | null;
  clientName?: string;
  phone?: string;
  categoryKey?: string;
  categoryName?: string;
  sourceServiceName?: string;
  lastRelevantVisitAt?: string;
  daysWithoutReturn?: number | null;
  lastProfessionalId?: number | null;
  lastProfessionalName?: string;
  lastProfessionalActive?: boolean | null;
  opportunityStatus?: string;
  priority?: "low" | "medium" | "high";
  owner?: string;
  notes?: string;
};

type CrmCatalogService = {
  serviceKey: string;
  serviceName: string;
  categoryKey?: string;
  categoryName?: string;
  serviceId?: number | null;
  durationMinutes?: number | null;
  price?: number | null;
  active?: boolean;
  visibleToClient?: boolean | null;
  rule?: CrmServiceRule | null;
};

type CrmDashboard = {
  settings?: CrmSettings;
  totals?: Record<string, number>;
  flowsByStatus?: Record<string, number>;
  opportunitiesByStatus?: Record<string, number>;
  topServices?: CrmServiceRule[];
  topCategories?: CrmCategoryRule[];
  recentBlocks?: CrmClientBlock[];
  recentFlows?: CrmFlowItem[];
  recentOpportunities?: CrmOpportunityItem[];
};

type CrmPreviewResult = {
  generatedAt?: string;
  materialize?: boolean;
  crmMode?: string;
  summary?: Record<string, number>;
  flowCandidates?: Array<Record<string, unknown>>;
  opportunityCandidates?: Array<Record<string, unknown>>;
  skipped?: Array<Record<string, unknown>>;
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

  private fileToDataUrl(file: File): Promise<string> {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => {
        const result = typeof reader.result === "string" ? reader.result : "";
        if (!result) {
          reject(new Error("Nao foi possivel ler a imagem selecionada."));
          return;
        }
        resolve(result);
      };
      reader.onerror = () => {
        reject(new Error("Falha ao carregar a imagem para upload."));
      };
      reader.readAsDataURL(file);
    });
  }

  async sendMessage(
    message: string,
    history: ChatHistoryItem[] = [],
    options: { tenantCode?: string } = {},
  ) {
    const tenantCode = String(options?.tenantCode || "").trim();
    if (!this.establishmentId && !tenantCode) {
      throw new Error("Configure tenantCode da sessao ou VITE_TRINKS_ESTABLISHMENT_ID.");
    }

    const response = await fetch(this.getBackendEndpoint("/api/chat"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ...(this.establishmentId ? { establishmentId: Number(this.establishmentId) } : {}),
        ...(tenantCode ? { tenantCode } : {}),
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

  async getKnowledge(adminToken = "", tenantCode = "") {
    const headers = new Headers({ "Content-Type": "application/json" });
    const normalizedToken = String(adminToken || "").trim();
    const normalizedTenantCode = String(tenantCode || "").trim();
    if (normalizedToken) {
      headers.set("x-admin-token", normalizedToken);
    }

    const query = new URLSearchParams();
    if (normalizedTenantCode) {
      query.set("tenantCode", normalizedTenantCode);
    }

    const response = await fetch(this.getBackendEndpoint(`/api/knowledge${query.toString() ? `?${query.toString()}` : ""}`), {
      method: "GET",
      headers,
    });

    if (!response.ok) {
      throw new Error(`Falha ao carregar base de conhecimento (${response.status}).`);
    }

    const data = (await response.json()) as { knowledge?: KnowledgePayload };
    return data.knowledge || {};
  }

  async saveKnowledge(knowledge: KnowledgePayload, adminToken = "", tenantCode = "") {
    const headers = new Headers({ "Content-Type": "application/json" });
    const normalizedToken = String(adminToken || "").trim();
    const normalizedTenantCode = String(tenantCode || "").trim();
    if (normalizedToken) {
      headers.set("x-admin-token", normalizedToken);
    }

    const response = await fetch(this.getBackendEndpoint("/api/knowledge"), {
      method: "PUT",
      headers,
      body: JSON.stringify({
        knowledge,
        ...(normalizedTenantCode ? { tenantCode: normalizedTenantCode } : {}),
      }),
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

  async uploadMarketingImage(file: File, adminToken: string, tenantCode = "") {
    if (!file) {
      throw new Error("Selecione uma imagem para upload.");
    }
    if (!file.type.startsWith("image/")) {
      throw new Error("Arquivo invalido. Envie uma imagem (png, jpg, webp ou gif).");
    }

    const imageDataUrl = await this.fileToDataUrl(file);
    const payload = await this.adminRequest<{
      url?: string;
      message?: string;
    }>("/api/admin/uploads/marketing-image", adminToken, {
      method: "POST",
      body: JSON.stringify({
        imageDataUrl,
        fileName: file.name,
        ...(String(tenantCode || "").trim() ? { tenantCode: String(tenantCode || "").trim() } : {}),
      }),
    });

    const url = String(payload?.url || "").trim();
    if (!url) {
      throw new Error(payload?.message || "Nao foi possivel obter a URL da imagem enviada.");
    }
    return url;
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

  async getWhatsappConversationsWithAuth(authToken: string) {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (authToken.trim()) {
      headers["x-admin-token"] = authToken.trim();
    }
    const response = await fetch(this.getBackendEndpoint("/api/whatsapp/inbox"), {
      method: "GET",
      headers,
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

  async getWhatsappMessagesWithAuth(phone: string, authToken: string) {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (authToken.trim()) {
      headers["x-admin-token"] = authToken.trim();
    }
    const response = await fetch(
      this.getBackendEndpoint(`/api/whatsapp/messages?phone=${encodeURIComponent(phone)}`),
      {
        method: "GET",
        headers,
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

  async getDbConversations(limit = 100, authToken = "") {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (authToken.trim()) {
      headers["x-admin-token"] = authToken.trim();
    }
    const response = await fetch(
      this.getBackendEndpoint(`/api/db/conversations?limit=${encodeURIComponent(String(limit))}`),
      {
        method: "GET",
        headers,
      },
    );

    if (!response.ok) {
      throw new Error(`Falha ao carregar conversas do banco (${response.status}).`);
    }

    const data = (await response.json()) as { data?: DbConversation[] };
    return Array.isArray(data.data) ? data.data : [];
  }

  async getDbMessages(phone: string, limit = 300, authToken = "") {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (authToken.trim()) {
      headers["x-admin-token"] = authToken.trim();
    }
    const response = await fetch(
      this.getBackendEndpoint(
        `/api/db/messages?phone=${encodeURIComponent(phone)}&limit=${encodeURIComponent(String(limit))}`,
      ),
      {
        method: "GET",
        headers,
      },
    );

    if (!response.ok) {
      throw new Error(`Falha ao carregar mensagens do banco (${response.status}).`);
    }

    const data = (await response.json()) as { messages?: DbMessage[] };
    return Array.isArray(data.messages) ? data.messages : [];
  }

  async getAppointmentsAudit(
    options: { phone?: string; status?: string; limit?: number } = {},
    authToken = "",
  ) {
    const params = new URLSearchParams();
    params.set("limit", String(options.limit ?? 300));
    if (options.phone) {
      params.set("phone", options.phone);
    }
    if (options.status) {
      params.set("status", options.status);
    }

    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (authToken.trim()) {
      headers["x-admin-token"] = authToken.trim();
    }
    const response = await fetch(
      this.getBackendEndpoint(`/api/db/appointments-audit?${params.toString()}`),
      {
        method: "GET",
        headers,
      },
    );

    if (!response.ok) {
      throw new Error(`Falha ao carregar auditoria (${response.status}).`);
    }

    const data = (await response.json()) as { data?: AppointmentAuditItem[] };
    return Array.isArray(data.data) ? data.data : [];
  }

  async getWebhookEvents(
    options: { phone?: string; status?: string; reason?: string; limit?: number } = {},
    authToken = "",
  ) {
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

    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (authToken.trim()) {
      headers["x-admin-token"] = authToken.trim();
    }
    const response = await fetch(
      this.getBackendEndpoint(`/api/db/webhook-events?${params.toString()}`),
      {
        method: "GET",
        headers,
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
      users?: AdminTenantUser[];
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

  async loginAdminTenantUser(payload: { tenantCode: string; username: string; password: string }) {
    const response = await fetch(this.getBackendEndpoint("/api/admin/auth/login"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      let message = `Falha no login do tenant (${response.status}).`;
      try {
        const data = (await response.json()) as { message?: string };
        if (data?.message) {
          message = data.message;
        }
      } catch {
        // Keep fallback message.
      }
      throw new Error(message);
    }

    return (await response.json()) as { token?: string; expiresAt?: string; principal?: AdminPrincipal };
  }

  async getAdminSession(adminToken: string) {
    return this.adminRequest<{ principal?: AdminPrincipal }>("/api/admin/auth/me", adminToken, {
      method: "GET",
    });
  }

  async logoutAdminSession(adminToken: string, allDevices = false) {
    return this.adminRequest<{ status?: string }>("/api/admin/auth/logout", adminToken, {
      method: "POST",
      body: JSON.stringify({ allDevices }),
    });
  }

  async getAdminTenantUsers(adminToken: string, code: string) {
    return this.adminRequest<{ users?: AdminTenantUser[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/users`,
      adminToken,
      { method: "GET" },
    );
  }

  async createAdminTenantUser(
    adminToken: string,
    code: string,
    payload: { username: string; displayName?: string; password: string; active?: boolean },
  ) {
    return this.adminRequest<{ users?: AdminTenantUser[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/users`,
      adminToken,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
    );
  }

  async updateAdminTenantUser(
    adminToken: string,
    code: string,
    userId: number,
    payload: { displayName?: string; password?: string; active?: boolean },
  ) {
    return this.adminRequest<{ users?: AdminTenantUser[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/users/${encodeURIComponent(String(userId))}`,
      adminToken,
      {
        method: "PUT",
        body: JSON.stringify(payload),
      },
    );
  }

  async getTenantCrmSettings(adminToken: string, code: string) {
    return this.adminRequest<{ settings?: CrmSettings }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/crm/settings`,
      adminToken,
      { method: "GET" },
    );
  }

  async saveTenantCrmSettings(adminToken: string, code: string, settings: CrmSettings) {
    return this.adminRequest<{ settings?: CrmSettings }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/crm/settings`,
      adminToken,
      {
        method: "PUT",
        body: JSON.stringify({ settings }),
      },
    );
  }

  async getTenantCrmServiceCatalog(adminToken: string, code: string) {
    return this.adminRequest<{ data?: CrmCatalogService[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/crm/services/catalog`,
      adminToken,
      { method: "GET" },
    );
  }

  async getTenantCrmServiceRules(adminToken: string, code: string) {
    return this.adminRequest<{ rules?: CrmServiceRule[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/crm/services/rules`,
      adminToken,
      { method: "GET" },
    );
  }

  async saveTenantCrmServiceRules(adminToken: string, code: string, rules: CrmServiceRule[]) {
    return this.adminRequest<{ rules?: CrmServiceRule[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/crm/services/rules`,
      adminToken,
      {
        method: "PUT",
        body: JSON.stringify({ rules }),
      },
    );
  }

  async getTenantCrmCategoryRules(adminToken: string, code: string) {
    return this.adminRequest<{ rules?: CrmCategoryRule[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/crm/categories/rules`,
      adminToken,
      { method: "GET" },
    );
  }

  async saveTenantCrmCategoryRules(adminToken: string, code: string, rules: CrmCategoryRule[]) {
    return this.adminRequest<{ rules?: CrmCategoryRule[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/crm/categories/rules`,
      adminToken,
      {
        method: "PUT",
        body: JSON.stringify({ rules }),
      },
    );
  }

  async getTenantCrmBlocks(adminToken: string, code: string) {
    return this.adminRequest<{ blocks?: CrmClientBlock[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/crm/blocks`,
      adminToken,
      { method: "GET" },
    );
  }

  async saveTenantCrmBlock(adminToken: string, code: string, payload: CrmClientBlock) {
    return this.adminRequest<{ block?: CrmClientBlock; blocks?: CrmClientBlock[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/crm/blocks`,
      adminToken,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
    );
  }

  async getTenantCrmFlows(
    adminToken: string,
    code: string,
    options: { status?: string; phone?: string; limit?: number } = {},
  ) {
    const params = new URLSearchParams();
    if (options.status) params.set("status", options.status);
    if (options.phone) params.set("phone", options.phone);
    if (options.limit != null) params.set("limit", String(options.limit));
    const suffix = params.toString() ? `?${params.toString()}` : "";
    return this.adminRequest<{ data?: CrmFlowItem[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/crm/flows${suffix}`,
      adminToken,
      { method: "GET" },
    );
  }

  async getTenantCrmOpportunities(
    adminToken: string,
    code: string,
    options: { status?: string; limit?: number } = {},
  ) {
    const params = new URLSearchParams();
    if (options.status) params.set("status", options.status);
    if (options.limit != null) params.set("limit", String(options.limit));
    const suffix = params.toString() ? `?${params.toString()}` : "";
    return this.adminRequest<{ data?: CrmOpportunityItem[] }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/crm/opportunities${suffix}`,
      adminToken,
      { method: "GET" },
    );
  }

  async getTenantCrmDashboard(adminToken: string, code: string) {
    return this.adminRequest<{ dashboard?: CrmDashboard }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/crm/dashboard`,
      adminToken,
      { method: "GET" },
    );
  }

  async runTenantCrmPreview(
    adminToken: string,
    code: string,
    payload: { lookbackDays?: number; limit?: number; materialize?: boolean } = {},
  ) {
    return this.adminRequest<{ preview?: CrmPreviewResult; dashboard?: CrmDashboard }>(
      `/api/admin/tenants/${encodeURIComponent(code)}/crm/preview-run`,
      adminToken,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
    );
  }
}

