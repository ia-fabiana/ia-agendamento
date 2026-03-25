# IA.AGENDAMENTO

Concierge digital para atendimento e agendamento com IA.

## Tecnologias
- Frontend: React + Vite + TypeScript
- IA: Google Gemini (function calling)
- Integrações externas: Backend seguro (Trinks + Evolution)

## Arquitetura recomendada
1. Frontend envia mensagem para IA.
2. IA identifica intenção de verificar disponibilidade ou agendar.
3. Frontend chama backend via endpoints seguros.
4. Backend chama API do Trinks com chave privada.
5. (Opcional) Backend responde via Evolution para WhatsApp.

## Configuração do frontend
1. Instale dependências:

```bash
npm install
```

2. Crie arquivo .env local baseado no .env.example:

```env
VITE_BACKEND_URL=http://localhost:3001
VITE_TRINKS_ESTABLISHMENT_ID=62260
```

3. Execute o frontend:

```bash
npm run dev
```

## Execução do backend
1. Preencha no .env:

```env
GEMINI_API_KEY=sua_chave_gemini
TRINKS_API_BASE_URL=https://api.trinks.com/v1
TRINKS_API_KEY=sua_chave_trinks
EVOLUTION_API_BASE_URL=https://api.iafabiana.com.br
EVOLUTION_API_KEY=sua_chave_evolution
EVOLUTION_INSTANCE=nome_da_instancia
PORT=3001
```

2. Rode o backend em outro terminal:

```bash
npm run dev:backend
```

3. Teste saúde:

```bash
GET http://localhost:3001/api/health
```

## Endpoints esperados no backend
O frontend foi preparado para consumir:

1. POST /api/trinks/availability
Payload:

```json
{
  "establishmentId": 62260,
  "service": "Corte Feminino",
  "date": "2026-03-18"
}
```

2. POST /api/trinks/appointments
Payload:

```json
{
  "establishmentId": 62260,
  "service": "Corte Feminino",
  "date": "2026-03-18",
  "time": "14:00",
  "professionalName": "Fabiana",
  "clientName": "Nome da Cliente",
  "clientPhone": "5511999999999"
}
```

`professionalName` é opcional. Se não for enviado, o backend seleciona a primeira profissional disponível para a data.

3. POST /api/evolution/send-text
Payload:

```json
{
  "to": "5511999999999",
  "text": "Sua mensagem"
}
```

4. POST /api/trinks/professionals
Payload:

```json
{
  "establishmentId": 62260,
  "date": "2026-03-18"
}
```

5. POST /api/trinks/appointments/reschedule
Payload:

```json
{
  "establishmentId": 62260,
  "confirmationCode": "TRK-123",
  "date": "2026-03-19",
  "time": "16:00"
}
```

Tambem aceita `appointmentId` numerico em vez de `confirmationCode`.

## Segurança
- Mantenha a chave do Gemini apenas no backend.
- Nunca exponha chave do Trinks ou da Evolution no frontend.
- Se qualquer chave foi compartilhada em chat, considere comprometida e rotacione imediatamente.
- Armazene segredos apenas no backend e no gerenciador de secrets do deploy.

## Deploy Frontend na Vercel (backend no Hetzner)
1. Conecte o repositório na Vercel e selecione este projeto.
2. Configure as variáveis de ambiente do frontend na Vercel:

```env
VITE_BACKEND_URL=https://204-168-176-220.sslip.io
VITE_TRINKS_ESTABLISHMENT_ID=62260
```

3. Build command: `npm run build`
4. Output directory: `dist`
5. Deploy.

Importante: como a Vercel serve em HTTPS, o backend também precisa estar em HTTPS para evitar bloqueio de mixed content no navegador.

## Base de conhecimento da IA
- Edite o arquivo [server/salonKnowledge.json](server/salonKnowledge.json) para ensinar respostas comerciais (servicos, politicas, FAQ, horarios e contatos).
- O backend carrega esse arquivo em cada mensagem da IA no endpoint de chat.
- Sempre que atualizar o JSON, a IA passa a usar o novo conteudo sem alterar o frontend.
