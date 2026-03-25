type ChatHistoryItem = {
  role: "user" | "assistant";
  content: string;
};

type KnowledgePayload = Record<string, unknown>;

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
}
