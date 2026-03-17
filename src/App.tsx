import React, { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { Send, Calendar, Sparkles, Phone, User, ChevronRight, MessageSquare } from 'lucide-react';
import { AppointmentService } from './services/appointmentService';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
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
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const appointmentService = useRef<AppointmentService | null>(null);

  useEffect(() => {
    appointmentService.current = new AppointmentService();
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
      const response = await appointmentService.current?.sendMessage(input);
      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: response || 'Desculpe, tive um pequeno contratempo. Poderia repetir?',
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error) {
      console.error('Error sending message:', error);
    } finally {
      setIsLoading(false);
    }
  };

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
        <div className="hidden sm:flex items-center gap-6 text-[11px] uppercase tracking-widest text-white/60">
          <span className="hover:text-white cursor-pointer transition-colors">Serviços</span>
          <span className="hover:text-white cursor-pointer transition-colors">O Salão</span>
          <span className="hover:text-white cursor-pointer transition-colors">Contato</span>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 max-w-4xl w-full mx-auto p-4 sm:p-8 flex flex-col gap-6 overflow-hidden">
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
