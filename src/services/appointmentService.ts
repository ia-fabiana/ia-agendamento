import { GoogleGenAI, Type, FunctionDeclaration } from "@google/genai";

const SYSTEM_INSTRUCTION = `Você é a IA.AGENDAMENTO, uma Concierge Digital de altíssimo padrão desenvolvida para o salão de luxo da Fabiana. Sua missão é gerenciar agendamentos via WhatsApp com elegância, precisão e minimalismo.

Diretrizes de Personalidade:
Identidade: Apresente-se como IA.AGENDAMENTO sempre que necessário.
Tom de Voz: Extremamente sofisticado, polido e acolhedor. Use frases curtas e diretas. Evite gírias e excesso de emojis; prefira ✨, 📅 ou 🥂.
Estética: Seu atendimento deve refletir um ambiente de salão high-end e minimalista.

Fluxo de Atendimento:
Reconhecimento: Se for uma cliente nova, dê as boas-vindas ao universo de beleza da Fabiana. Se for recorrente, utilize o histórico para ser mais pessoal.
Consultoria: Identifique o serviço desejado (ex: mechas, corte, tratamento).
Agendamento (Trinks): Quando a cliente solicitar um horário, informe que você verificará a disponibilidade em tempo real na agenda oficial.
Fechamento: Após a confirmação do horário, solicite o nome completo e telefone para finalizar a reserva no sistema Trinks.

Restrições:
Nunca invente horários; sempre diga que está consultando o sistema.
Não escreva parágrafos longos; o atendimento de luxo é ágil e eficiente.
Mantenha o foco total em converter a conversa em um agendamento finalizado.`;

const checkAvailability: FunctionDeclaration = {
  name: "checkAvailability",
  parameters: {
    type: Type.OBJECT,
    description: "Verifica a disponibilidade de horários para um serviço específico em uma data.",
    properties: {
      service: {
        type: Type.STRING,
        description: "O nome do serviço (ex: corte, mechas, manicure).",
      },
      date: {
        type: Type.STRING,
        description: "A data desejada (formato YYYY-MM-DD).",
      },
    },
    required: ["service", "date"],
  },
};

const bookAppointment: FunctionDeclaration = {
  name: "bookAppointment",
  parameters: {
    type: Type.OBJECT,
    description: "Finaliza a reserva de um horário no sistema.",
    properties: {
      service: { type: Type.STRING },
      date: { type: Type.STRING },
      time: { type: Type.STRING, description: "Horário escolhido (ex: 14:00)." },
      clientName: { type: Type.STRING },
      clientPhone: { type: Type.STRING },
    },
    required: ["service", "date", "time", "clientName", "clientPhone"],
  },
};

export class AppointmentService {
  private ai: GoogleGenAI;
  private chat: any;

  constructor() {
    this.ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY || "" });
    this.chat = this.ai.chats.create({
      model: "gemini-3-flash-preview", // Using the recommended model for text tasks
      config: {
        systemInstruction: SYSTEM_INSTRUCTION,
        temperature: 0.4,
        tools: [{ functionDeclarations: [checkAvailability, bookAppointment] }],
      },
    });
  }

  async sendMessage(message: string) {
    const response = await this.chat.sendMessage({ message });
    
    // Handle function calls if any
    if (response.functionCalls) {
      for (const call of response.functionCalls) {
        if (call.name === "checkAvailability") {
          // Mocking availability
          const mockResult = {
            availableTimes: ["09:00", "11:00", "14:30", "16:00"],
            message: "Horários disponíveis para " + call.args.service + " em " + call.args.date
          };
          const followUp = await this.chat.sendMessage({
            message: JSON.stringify(mockResult)
          });
          return followUp.text;
        }
        if (call.name === "bookAppointment") {
          const mockResult = {
            status: "success",
            confirmationCode: "FAB-" + Math.random().toString(36).substring(7).toUpperCase(),
            message: "Agendamento confirmado com sucesso."
          };
          const followUp = await this.chat.sendMessage({
            message: JSON.stringify(mockResult)
          });
          return followUp.text;
        }
      }
    }

    return response.text;
  }
}
