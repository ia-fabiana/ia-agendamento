type ChatHistoryItem = {
  role: "user" | "assistant";
  content: string;
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
}
