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

function toText(value: unknown) {
  return typeof value === 'string' ? value : '';
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

  const updateKnowledge = (updater: (draft: Record<string, any>) => void) => {
    try {
      const draft = safeParseKnowledge(knowledgeJson) as Record<string, any>;
      updater(draft);
      setKnowledgeJson(JSON.stringify(draft, null, 2));
      setKnowledgeStatus('Edicao local atualizada. Clique em Salvar para persistir.');
    } catch {
      setKnowledgeStatus('JSON invalido na aba Base. Corrija o JSON para continuar editando pelos formulários.');
    }
  };

  const services = Array.isArray(knowledgeObject?.services) ? knowledgeObject.services : [];
  const faq = Array.isArray(knowledgeObject?.faq) ? knowledgeObject.faq : [];
  const paymentMethods = Array.isArray(knowledgeObject?.business?.paymentMethods)
    ? knowledgeObject.business.paymentMethods
    : [];
  const knowledgeStatusClass = knowledgeStatus.toLowerCase().includes('erro')
    ? 'text-red-300'
    : 'text-brand-green';

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
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <h2 className="heading-bold text-lg text-white">Servicos</h2>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() =>
                        updateKnowledge((draft) => {
                          const current = Array.isArray(draft.services) ? draft.services : [];
                          draft.services = [...current, { name: '', durationMinutes: 60, price: '' }];
                        })
                      }
                      className="px-3 py-2 rounded-lg bg-white/10 text-white text-xs uppercase tracking-wider"
                    >
                      + Servico
                    </button>
                    <button
                      onClick={handleSaveKnowledge}
                      disabled={isSavingKnowledge || isLoadingKnowledge}
                      className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                    >
                      {isSavingKnowledge ? 'Salvando...' : 'Salvar'}
                    </button>
                  </div>
                </div>
                {knowledgeStatus && <p className={`text-xs ${knowledgeStatusClass}`}>{knowledgeStatus}</p>}
                {!services.length && <p className="text-white/70 text-sm">Nenhum servico cadastrado. Clique em + Servico.</p>}
                <div className="grid sm:grid-cols-2 gap-3">
                  {services.map((item: any, idx: number) => (
                    <div key={`${item?.name || 'servico'}-${idx}`} className="rounded-xl bg-white/5 border border-white/10 p-4">
                      <div className="space-y-2">
                        <input
                          value={toText(item?.name)}
                          onChange={(e) =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.services) ? draft.services : [];
                              current[idx] = { ...(current[idx] || {}), name: e.target.value };
                              draft.services = current;
                            })
                          }
                          placeholder="Nome do servico"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                        />
                        <input
                          value={String(item?.durationMinutes ?? '')}
                          onChange={(e) =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.services) ? draft.services : [];
                              current[idx] = {
                                ...(current[idx] || {}),
                                durationMinutes: Number(e.target.value || 0),
                              };
                              draft.services = current;
                            })
                          }
                          placeholder="Duracao em minutos"
                          type="number"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                        />
                        <input
                          value={toText(item?.price)}
                          onChange={(e) =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.services) ? draft.services : [];
                              current[idx] = { ...(current[idx] || {}), price: e.target.value };
                              draft.services = current;
                            })
                          }
                          placeholder="Preco (ex: a partir de 180)"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                        />
                        <button
                          onClick={() =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.services) ? draft.services : [];
                              draft.services = current.filter((_: any, currentIdx: number) => currentIdx !== idx);
                            })
                          }
                          className="text-xs text-red-300 hover:text-red-200"
                        >
                          Remover servico
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              </>
            )}

            {activeSection === 'salon' && (
              <>
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <h2 className="heading-bold text-lg text-white">O Salao</h2>
                  <button
                    onClick={handleSaveKnowledge}
                    disabled={isSavingKnowledge || isLoadingKnowledge}
                    className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                  >
                    {isSavingKnowledge ? 'Salvando...' : 'Salvar'}
                  </button>
                </div>
                {knowledgeStatus && <p className={`text-xs ${knowledgeStatusClass}`}>{knowledgeStatus}</p>}
                <div className="grid sm:grid-cols-2 gap-3">
                  <input
                    value={toText(knowledgeObject?.identity?.brandName)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.identity = { ...(draft.identity || {}), brandName: e.target.value };
                      })
                    }
                    placeholder="Nome comercial"
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <input
                    value={toText(knowledgeObject?.identity?.toneGuide)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.identity = { ...(draft.identity || {}), toneGuide: e.target.value };
                      })
                    }
                    placeholder="Guia de tom"
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <input
                    value={toText(knowledgeObject?.business?.address)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.business = { ...(draft.business || {}), address: e.target.value };
                      })
                    }
                    placeholder="Endereco"
                    className="sm:col-span-2 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <input
                    value={toText(knowledgeObject?.business?.openingHours)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.business = { ...(draft.business || {}), openingHours: e.target.value };
                      })
                    }
                    placeholder="Horario de funcionamento"
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <input
                    value={toText(knowledgeObject?.business?.phone)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.business = { ...(draft.business || {}), phone: e.target.value };
                      })
                    }
                    placeholder="Telefone"
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <textarea
                    value={toText(knowledgeObject?.policies?.latePolicy)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.policies = { ...(draft.policies || {}), latePolicy: e.target.value };
                      })
                    }
                    placeholder="Politica de atraso"
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm min-h-20"
                  />
                  <textarea
                    value={toText(knowledgeObject?.policies?.cancellationPolicy)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.policies = { ...(draft.policies || {}), cancellationPolicy: e.target.value };
                      })
                    }
                    placeholder="Politica de cancelamento"
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm min-h-20"
                  />
                  <textarea
                    value={toText(knowledgeObject?.policies?.noShowPolicy)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.policies = { ...(draft.policies || {}), noShowPolicy: e.target.value };
                      })
                    }
                    placeholder="Politica de no-show"
                    className="sm:col-span-2 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm min-h-20"
                  />
                </div>
              </>
            )}

            {activeSection === 'contact' && (
              <>
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <h2 className="heading-bold text-lg text-white">Contato</h2>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() =>
                        updateKnowledge((draft) => {
                          const current = Array.isArray(draft.faq) ? draft.faq : [];
                          draft.faq = [...current, { question: '', answer: '' }];
                        })
                      }
                      className="px-3 py-2 rounded-lg bg-white/10 text-white text-xs uppercase tracking-wider"
                    >
                      + FAQ
                    </button>
                    <button
                      onClick={handleSaveKnowledge}
                      disabled={isSavingKnowledge || isLoadingKnowledge}
                      className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                    >
                      {isSavingKnowledge ? 'Salvando...' : 'Salvar'}
                    </button>
                  </div>
                </div>
                {knowledgeStatus && <p className={`text-xs ${knowledgeStatusClass}`}>{knowledgeStatus}</p>}
                <div className="space-y-3 text-sm text-white/80">
                  <input
                    value={toText(knowledgeObject?.business?.phone)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.business = { ...(draft.business || {}), phone: e.target.value };
                      })
                    }
                    placeholder="Telefone de contato"
                    className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <input
                    value={paymentMethods.join(', ')}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        const methods = e.target.value
                          .split(',')
                          .map((item) => item.trim())
                          .filter(Boolean);
                        draft.business = { ...(draft.business || {}), paymentMethods: methods };
                      })
                    }
                    placeholder="Formas de pagamento separadas por virgula"
                    className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                </div>
                {faq.length > 0 && (
                  <div className="mt-4 space-y-2">
                    {faq.map((item: any, idx: number) => (
                      <div key={`faq-${idx}`} className="rounded-lg bg-white/5 border border-white/10 p-3">
                        <p className="text-xs uppercase tracking-wider text-brand-blue">Pergunta</p>
                        <input
                          value={toText(item?.question)}
                          onChange={(e) =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.faq) ? draft.faq : [];
                              current[idx] = { ...(current[idx] || {}), question: e.target.value };
                              draft.faq = current;
                            })
                          }
                          placeholder="Pergunta frequente"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm mt-1"
                        />
                        <p className="text-xs uppercase tracking-wider text-brand-green mt-2">Resposta</p>
                        <textarea
                          value={toText(item?.answer)}
                          onChange={(e) =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.faq) ? draft.faq : [];
                              current[idx] = { ...(current[idx] || {}), answer: e.target.value };
                              draft.faq = current;
                            })
                          }
                          placeholder="Resposta da pergunta"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm mt-1 min-h-20"
                        />
                        <button
                          onClick={() =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.faq) ? draft.faq : [];
                              draft.faq = current.filter((_: any, currentIdx: number) => currentIdx !== idx);
                            })
                          }
                          className="text-xs text-red-300 hover:text-red-200 mt-2"
                        >
                          Remover FAQ
                        </button>
                      </div>
                    ))}
                  </div>
                )}
                {!faq.length && <p className="text-white/70 text-sm">Nenhum FAQ cadastrado. Clique em + FAQ.</p>}
              </>
            )}

            {activeSection === 'knowledge' && (
              <>
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <h2 className="heading-bold text-lg text-white">Base de Conhecimento</h2>
                  <button
                    onClick={handleSaveKnowledge}
                    disabled={isSavingKnowledge || isLoadingKnowledge}
                    className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                  >
                    {isSavingKnowledge ? 'Salvando...' : 'Salvar'}
                  </button>
                </div>
                <p className="text-xs text-white/60">
                  Preencha os campos abaixo e clique em Salvar. A IA usa esta base em tempo real.
                </p>
                {knowledgeStatus && <p className={`text-xs ${knowledgeStatusClass}`}>{knowledgeStatus}</p>}

                <div className="grid sm:grid-cols-2 gap-3">
                  <input
                    value={toText(knowledgeObject?.identity?.brandName)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.identity = { ...(draft.identity || {}), brandName: e.target.value };
                      })
                    }
                    placeholder="Nome comercial"
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <input
                    value={toText(knowledgeObject?.identity?.toneGuide)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.identity = { ...(draft.identity || {}), toneGuide: e.target.value };
                      })
                    }
                    placeholder="Guia de tom"
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <input
                    value={toText(knowledgeObject?.business?.address)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.business = { ...(draft.business || {}), address: e.target.value };
                      })
                    }
                    placeholder="Endereco"
                    className="sm:col-span-2 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <input
                    value={toText(knowledgeObject?.business?.openingHours)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.business = { ...(draft.business || {}), openingHours: e.target.value };
                      })
                    }
                    placeholder="Horario de funcionamento"
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <input
                    value={toText(knowledgeObject?.business?.phone)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.business = { ...(draft.business || {}), phone: e.target.value };
                      })
                    }
                    placeholder="Telefone"
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <input
                    value={paymentMethods.join(', ')}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        const methods = e.target.value
                          .split(',')
                          .map((item) => item.trim())
                          .filter(Boolean);
                        draft.business = { ...(draft.business || {}), paymentMethods: methods };
                      })
                    }
                    placeholder="Formas de pagamento separadas por virgula"
                    className="sm:col-span-2 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <textarea
                    value={toText(knowledgeObject?.policies?.latePolicy)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.policies = { ...(draft.policies || {}), latePolicy: e.target.value };
                      })
                    }
                    placeholder="Politica de atraso"
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm min-h-20"
                  />
                  <textarea
                    value={toText(knowledgeObject?.policies?.cancellationPolicy)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.policies = { ...(draft.policies || {}), cancellationPolicy: e.target.value };
                      })
                    }
                    placeholder="Politica de cancelamento"
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm min-h-20"
                  />
                  <textarea
                    value={toText(knowledgeObject?.policies?.noShowPolicy)}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        draft.policies = { ...(draft.policies || {}), noShowPolicy: e.target.value };
                      })
                    }
                    placeholder="Politica de no-show"
                    className="sm:col-span-2 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm min-h-20"
                  />
                </div>

                <div className="flex items-center justify-between gap-3 flex-wrap pt-4">
                  <h3 className="heading-bold text-base text-white">Servicos</h3>
                  <button
                    onClick={() =>
                      updateKnowledge((draft) => {
                        const current = Array.isArray(draft.services) ? draft.services : [];
                        draft.services = [...current, { name: '', durationMinutes: 60, price: '' }];
                      })
                    }
                    className="px-3 py-2 rounded-lg bg-white/10 text-white text-xs uppercase tracking-wider"
                  >
                    + Servico
                  </button>
                </div>
                {!services.length && (
                  <p className="text-white/70 text-sm">Nenhum servico cadastrado. Clique em + Servico.</p>
                )}
                <div className="grid sm:grid-cols-2 gap-3">
                  {services.map((item: any, idx: number) => (
                    <div key={`${item?.name || 'servico'}-${idx}`} className="rounded-xl bg-white/5 border border-white/10 p-4">
                      <div className="space-y-2">
                        <input
                          value={toText(item?.name)}
                          onChange={(e) =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.services) ? draft.services : [];
                              current[idx] = { ...(current[idx] || {}), name: e.target.value };
                              draft.services = current;
                            })
                          }
                          placeholder="Nome do servico"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                        />
                        <input
                          value={String(item?.durationMinutes ?? '')}
                          onChange={(e) =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.services) ? draft.services : [];
                              current[idx] = {
                                ...(current[idx] || {}),
                                durationMinutes: Number(e.target.value || 0),
                              };
                              draft.services = current;
                            })
                          }
                          placeholder="Duracao em minutos"
                          type="number"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                        />
                        <input
                          value={toText(item?.price)}
                          onChange={(e) =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.services) ? draft.services : [];
                              current[idx] = { ...(current[idx] || {}), price: e.target.value };
                              draft.services = current;
                            })
                          }
                          placeholder="Preco (ex: a partir de 180)"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                        />
                        <button
                          onClick={() =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.services) ? draft.services : [];
                              draft.services = current.filter((_: any, currentIdx: number) => currentIdx !== idx);
                            })
                          }
                          className="text-xs text-red-300 hover:text-red-200"
                        >
                          Remover servico
                        </button>
                      </div>
                    </div>
                  ))}
                </div>

                <div className="flex items-center justify-between gap-3 flex-wrap pt-4">
                  <h3 className="heading-bold text-base text-white">FAQ</h3>
                  <button
                    onClick={() =>
                      updateKnowledge((draft) => {
                        const current = Array.isArray(draft.faq) ? draft.faq : [];
                        draft.faq = [...current, { question: '', answer: '' }];
                      })
                    }
                    className="px-3 py-2 rounded-lg bg-white/10 text-white text-xs uppercase tracking-wider"
                  >
                    + FAQ
                  </button>
                </div>
                {faq.length > 0 && (
                  <div className="space-y-2">
                    {faq.map((item: any, idx: number) => (
                      <div key={`faq-base-${idx}`} className="rounded-lg bg-white/5 border border-white/10 p-3">
                        <p className="text-xs uppercase tracking-wider text-brand-blue">Pergunta</p>
                        <input
                          value={toText(item?.question)}
                          onChange={(e) =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.faq) ? draft.faq : [];
                              current[idx] = { ...(current[idx] || {}), question: e.target.value };
                              draft.faq = current;
                            })
                          }
                          placeholder="Pergunta frequente"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm mt-1"
                        />
                        <p className="text-xs uppercase tracking-wider text-brand-green mt-2">Resposta</p>
                        <textarea
                          value={toText(item?.answer)}
                          onChange={(e) =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.faq) ? draft.faq : [];
                              current[idx] = { ...(current[idx] || {}), answer: e.target.value };
                              draft.faq = current;
                            })
                          }
                          placeholder="Resposta da pergunta"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm mt-1 min-h-20"
                        />
                        <button
                          onClick={() =>
                            updateKnowledge((draft) => {
                              const current = Array.isArray(draft.faq) ? draft.faq : [];
                              draft.faq = current.filter((_: any, currentIdx: number) => currentIdx !== idx);
                            })
                          }
                          className="text-xs text-red-300 hover:text-red-200 mt-2"
                        >
                          Remover FAQ
                        </button>
                      </div>
                    ))}
                  </div>
                )}
                {!faq.length && <p className="text-white/70 text-sm">Nenhum FAQ cadastrado. Clique em + FAQ.</p>}
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
