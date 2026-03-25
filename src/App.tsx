import React, { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { Send, Calendar, Sparkles, Phone } from 'lucide-react';
import { AppointmentService } from './services/appointmentService';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
}

type TopSection = 'chat' | 'services' | 'salon' | 'contact' | 'knowledge';

const initialKnowledge = {
  identity: {
    brandName: 'Fabiana Luxury Salon',
    toneGuide: 'sofisticado, acolhedor, objetivo',
  },
  business: {
    address: '',
    phone: '',
    openingHours: '',
    paymentMethods: ['PIX', 'Cartao de credito', 'Cartao de debito'],
  },
  policies: {
    latePolicy: '',
    cancellationPolicy: '',
    noShowPolicy: '',
  },
  services: [],
  faq: [],
};

function safeParseKnowledge(raw: string) {
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('A base de conhecimento deve ser um objeto JSON.');
  }
  return parsed;
}

export default function App() {
  const [messages, setMessages] = useState<Message[]>([
    {
      id: '1',
      role: 'assistant',
      content: 'Bem-vinda ao universo de beleza da Fabiana. Sou a IA.AGENDAMENTO, sua concierge digital. Como posso tornar seu dia mais especial hoje? ✨',
      timestamp: new Date(),
    },
  ]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [activeSection, setActiveSection] = useState<TopSection>('chat');
  const [knowledgeJson, setKnowledgeJson] = useState(JSON.stringify(initialKnowledge, null, 2));
  const [knowledgeStatus, setKnowledgeStatus] = useState('');
  const [isSavingKnowledge, setIsSavingKnowledge] = useState(false);
  const [isLoadingKnowledge, setIsLoadingKnowledge] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const appointmentService = useRef<AppointmentService | null>(null);

  useEffect(() => {
    appointmentService.current = new AppointmentService();

    const loadKnowledge = async () => {
      if (!appointmentService.current) return;
      setIsLoadingKnowledge(true);
      try {
        const knowledge = await appointmentService.current.getKnowledge();
        setKnowledgeJson(JSON.stringify(knowledge, null, 2));
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Erro ao carregar base de conhecimento.';
        setKnowledgeStatus(message);
      } finally {
        setIsLoadingKnowledge(false);
      }
    };

    loadKnowledge();
  }, []);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleSend = async () => {
    if (!input.trim() || isLoading) return;

    const userMessage: Message = {
      id: Date.now().toString(),
      role: 'user',
      content: input,
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);

    try {
      const response = await appointmentService.current?.sendMessage(
        input,
        messages.map((message) => ({
          role: message.role,
          content: message.content,
        })),
      );
      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: response || 'Desculpe, tive um pequeno contratempo. Poderia repetir?',
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error) {
      console.error('Error sending message:', error);
      const fallbackMessage =
        'Nao foi possivel concluir sua mensagem agora. Verifique a conexao com o servidor e tente novamente.';
      const content = error instanceof Error && error.message ? error.message : fallbackMessage;
      const errorMessage: Message = {
        id: (Date.now() + 2).toString(),
        role: 'assistant',
        content,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSaveKnowledge = async () => {
    if (!appointmentService.current || isSavingKnowledge) return;
    setIsSavingKnowledge(true);
    setKnowledgeStatus('Salvando base de conhecimento...');

    try {
      const parsed = safeParseKnowledge(knowledgeJson);
      const saved = await appointmentService.current.saveKnowledge(parsed);
      setKnowledgeJson(JSON.stringify(saved, null, 2));
      setKnowledgeStatus('Base de conhecimento salva com sucesso.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao salvar base de conhecimento.';
      setKnowledgeStatus(message);
    } finally {
      setIsSavingKnowledge(false);
    }
  };

  const knowledgeObject = (() => {
    try {
      return safeParseKnowledge(knowledgeJson) as Record<string, any>;
    } catch {
      return initialKnowledge;
    }
  })();

  const services = Array.isArray(knowledgeObject?.services) ? knowledgeObject.services : [];
  const faq = Array.isArray(knowledgeObject?.faq) ? knowledgeObject.faq : [];

  return (
    <div className="min-h-screen flex flex-col luxury-gradient">
      {/* Header */}
      <header className="p-6 border-bottom border-white/5 flex items-center justify-between sticky top-0 z-10 glass">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-full bg-brand-purple/20 flex items-center justify-center border border-brand-purple/30">
            <Sparkles className="w-5 h-5 text-brand-purple" />
          </div>
          <div>
            <h1 className="heading-bold text-xl text-white/90">IA.AGENDAMENTO</h1>
            <p className="label-micro text-brand-blue">Concierge de Luxo</p>
          </div>
        </div>
        <div className="hidden sm:flex items-center gap-5 text-[11px] uppercase tracking-widest text-white/60">
          <button onClick={() => setActiveSection('services')} className="hover:text-white cursor-pointer transition-colors">Servicos</button>
          <button onClick={() => setActiveSection('salon')} className="hover:text-white cursor-pointer transition-colors">O Salao</button>
          <button onClick={() => setActiveSection('contact')} className="hover:text-white cursor-pointer transition-colors">Contato</button>
          <button onClick={() => setActiveSection('knowledge')} className="hover:text-white cursor-pointer transition-colors">Base</button>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 max-w-4xl w-full mx-auto p-4 sm:p-8 flex flex-col gap-6 overflow-hidden">
        {activeSection !== 'chat' && (
          <div className="glass rounded-2xl p-5 sm:p-6 space-y-4">
            {activeSection === 'services' && (
              <>
                <h2 className="heading-bold text-lg text-white">Servicos</h2>
                {!services.length && <p className="text-white/70 text-sm">Nenhum servico cadastrado na base de conhecimento.</p>}
                <div className="grid sm:grid-cols-2 gap-3">
                  {services.map((item: any, idx: number) => (
                    <div key={`${item?.name || 'servico'}-${idx}`} className="rounded-xl bg-white/5 border border-white/10 p-4">
                      <p className="font-display font-bold text-white/90">{item?.name || 'Servico'}</p>
                      <p className="text-xs text-white/60 mt-1">Duracao: {item?.durationMinutes || '-'} min</p>
                      <p className="text-xs text-brand-blue mt-1">Preco: {item?.price || 'sob consulta'}</p>
                    </div>
                  ))}
                </div>
              </>
            )}

            {activeSection === 'salon' && (
              <>
                <h2 className="heading-bold text-lg text-white">O Salao</h2>
                <div className="space-y-2 text-sm text-white/80">
                  <p><strong className="text-white/95">Nome:</strong> {knowledgeObject?.identity?.brandName || 'Nao informado'}</p>
                  <p><strong className="text-white/95">Endereco:</strong> {knowledgeObject?.business?.address || 'Nao informado'}</p>
                  <p><strong className="text-white/95">Horarios:</strong> {knowledgeObject?.business?.openingHours || 'Nao informado'}</p>
                  <p><strong className="text-white/95">Politica de atraso:</strong> {knowledgeObject?.policies?.latePolicy || 'Nao informado'}</p>
                  <p><strong className="text-white/95">Cancelamento:</strong> {knowledgeObject?.policies?.cancellationPolicy || 'Nao informado'}</p>
                </div>
              </>
            )}

            {activeSection === 'contact' && (
              <>
                <h2 className="heading-bold text-lg text-white">Contato</h2>
                <div className="space-y-2 text-sm text-white/80">
                  <p><strong className="text-white/95">Telefone:</strong> {knowledgeObject?.business?.phone || 'Nao informado'}</p>
                  <p><strong className="text-white/95">Pagamento:</strong> {Array.isArray(knowledgeObject?.business?.paymentMethods) && knowledgeObject.business.paymentMethods.length ? knowledgeObject.business.paymentMethods.join(', ') : 'Nao informado'}</p>
                </div>
                {faq.length > 0 && (
                  <div className="mt-4 space-y-2">
                    {faq.map((item: any, idx: number) => (
                      <div key={`faq-${idx}`} className="rounded-lg bg-white/5 border border-white/10 p-3">
                        <p className="text-xs uppercase tracking-wider text-brand-blue">Pergunta</p>
                        <p className="text-sm text-white/90">{item?.question}</p>
                        <p className="text-xs uppercase tracking-wider text-brand-green mt-2">Resposta</p>
                        <p className="text-sm text-white/80">{item?.answer}</p>
                      </div>
                    ))}
                  </div>
                )}
              </>
            )}

            {activeSection === 'knowledge' && (
              <>
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <h2 className="heading-bold text-lg text-white">Editar Base de Conhecimento</h2>
                  <button
                    onClick={handleSaveKnowledge}
                    disabled={isSavingKnowledge || isLoadingKnowledge}
                    className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                  >
                    {isSavingKnowledge ? 'Salvando...' : 'Salvar'}
                  </button>
                </div>
                <p className="text-xs text-white/60">Edite o JSON abaixo e clique em Salvar. Isso atualiza a base usada pela IA em tempo real.</p>
                <textarea
                  value={knowledgeJson}
                  onChange={(e) => setKnowledgeJson(e.target.value)}
                  className="w-full min-h-72 rounded-xl bg-[#0f1731] border border-white/15 text-white/90 text-xs p-3 font-mono"
                />
                {knowledgeStatus && <p className="text-xs text-brand-green">{knowledgeStatus}</p>}
              </>
            )}

            <button
              onClick={() => setActiveSection('chat')}
              className="label-micro text-white/70 hover:text-white"
            >
              Voltar para chat
            </button>
          </div>
        )}

        <div className="flex-1 overflow-y-auto pr-2 space-y-8 scrollbar-hide">
          <AnimatePresence initial={false}>
            {messages.map((msg) => (
              <motion.div
                key={msg.id}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
              >
                <div
                  className={`max-w-[85%] sm:max-w-[70%] p-5 rounded-2xl ${
                    msg.role === 'user'
                      ? 'bg-gradient-to-br from-brand-purple to-brand-blue text-white rounded-tr-none shadow-lg shadow-brand-purple/20'
                      : 'glass rounded-tl-none text-white/90 font-light leading-relaxed'
                  }`}
                >
                  <p className={msg.role === 'assistant' ? 'font-display font-bold text-lg tracking-tight' : 'text-sm font-medium'}>
                    {msg.content}
                  </p>
                  <span className={`label-micro mt-2 block opacity-40 ${msg.role === 'user' ? 'text-white' : 'text-white'}`}>
                    {msg.timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  </span>
                </div>
              </motion.div>
            ))}
          </AnimatePresence>
          {isLoading && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              className="flex justify-start"
            >
              <div className="glass p-4 rounded-2xl rounded-tl-none flex gap-1">
                <span className="w-1.5 h-1.5 bg-white/40 rounded-full animate-bounce" />
                <span className="w-1.5 h-1.5 bg-white/40 rounded-full animate-bounce [animation-delay:0.2s]" />
                <span className="w-1.5 h-1.5 bg-white/40 rounded-full animate-bounce [animation-delay:0.4s]" />
              </div>
            </motion.div>
          )}
          <div ref={messagesEndRef} />
        </div>

        {/* Input Area */}
        <div className="relative group">
          <div className="absolute -inset-0.5 bg-gradient-to-r from-white/10 to-white/5 rounded-2xl blur opacity-30 group-hover:opacity-50 transition duration-1000"></div>
          <div className="relative glass rounded-2xl p-2 flex items-center gap-2">
            <input
              type="text"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyPress={(e) => e.key === 'Enter' && handleSend()}
              placeholder="Como posso ajudar você hoje?"
              className="flex-1 bg-transparent border-none focus:ring-0 text-white placeholder:text-white/20 px-4 py-3 text-sm"
            />
            <button
              onClick={handleSend}
              disabled={isLoading || !input.trim()}
              aria-label="Enviar mensagem"
              title="Enviar mensagem"
              className="w-10 h-10 rounded-xl bg-gradient-to-br from-brand-purple to-brand-blue text-white flex items-center justify-center hover:opacity-90 transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-lg shadow-brand-purple/20"
            >
              <Send className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* Quick Actions */}
        <div className="flex flex-wrap gap-3 justify-center">
          <button 
            onClick={() => setInput('Gostaria de agendar um horário')}
            className="px-4 py-2 rounded-full glass label-micro text-white/60 hover:text-brand-purple hover:border-brand-purple/30 transition-all flex items-center gap-2"
          >
            <Calendar className="w-3 h-3 text-brand-purple" /> Agendar Horário
          </button>
          <button 
            onClick={() => setInput('Quais são os serviços disponíveis?')}
            className="px-4 py-2 rounded-full glass label-micro text-white/60 hover:text-brand-blue hover:border-brand-blue/30 transition-all flex items-center gap-2"
          >
            <Sparkles className="w-3 h-3 text-brand-blue" /> Ver Serviços
          </button>
          <button 
            onClick={() => setInput('Falar com um atendente')}
            className="px-4 py-2 rounded-full glass label-micro text-white/60 hover:text-brand-green hover:border-brand-green/30 transition-all flex items-center gap-2"
          >
            <Phone className="w-3 h-3 text-brand-green" /> Suporte Humano
          </button>
          <button
            onClick={() => setActiveSection('knowledge')}
            className="px-4 py-2 rounded-full glass label-micro text-white/60 hover:text-white hover:border-white/30 transition-all flex items-center gap-2"
          >
            <Sparkles className="w-3 h-3 text-white/70" /> Editar Base
          </button>
        </div>
      </main>

      {/* Footer */}
      <footer className="p-6 text-center">
        <p className="label-micro text-white/20">
          Fabiana Luxury Salon &copy; {new Date().getFullYear()}
        </p>
      </footer>
    </div>
  );
}
