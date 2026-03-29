import React, { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { Send, Calendar, Sparkles, Phone } from 'lucide-react';
import { AppointmentService } from './services/appointmentService';
import * as XLSX from 'xlsx';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
}

type TopSection = 'chat' | 'services' | 'salon' | 'contact' | 'knowledge' | 'inbox' | 'history' | 'admin';
type EvolutionStatus = {
  status?: string;
  instance?: string;
  connected?: boolean;
  data?: Record<string, unknown> | null;
};

type EvolutionQr = {
  status?: string;
  instance?: string;
  qr?: {
    hasQrImage?: boolean;
    qrDataUrl?: string | null;
    pairingCode?: string | null;
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
  role: 'user' | 'assistant';
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
  role: 'user' | 'assistant';
  content: string;
  senderName?: string;
  at?: string;
  source?: string;
};

type AuditItem = {
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
  requestPayload?: Record<string, any> | null;
  responsePayload?: Record<string, any> | null;
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
  details?: Record<string, any> | null;
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
};

type AdminTenantIdentifier = {
  id?: number;
  kind: string;
  value: string;
  normalizedValue?: string;
};

type AdminTenantProviderConfig = {
  id?: number;
  provider: string;
  enabled: boolean;
  config?: Record<string, any>;
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
};

type AdminPrincipal = {
  role: 'superadmin' | 'tenant';
  tokenType?: string;
  tenantCode?: string;
  tenantName?: string;
  username?: string;
  displayName?: string;
  expiresAt?: string;
};

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

function normalizeDigits(value: string) {
  return String(value || '').replace(/\D/g, '');
}

const ADMIN_TOKEN_STORAGE_KEY = 'ia_agendamento_admin_token';
const TENANT_LOGIN_LAST_STORAGE_KEY = 'ia_agendamento_tenant_last_login';

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
  const [whatsappInstance, setWhatsappInstance] = useState('ia-agendamento');
  const [whatsappStatus, setWhatsappStatus] = useState<EvolutionStatus | null>(null);
  const [whatsappQr, setWhatsappQr] = useState<EvolutionQr | null>(null);
  const [whatsappError, setWhatsappError] = useState('');
  const [isLoadingWhatsapp, setIsLoadingWhatsapp] = useState(false);
  const [inboxConversations, setInboxConversations] = useState<WhatsappConversation[]>([]);
  const [inboxMessages, setInboxMessages] = useState<WhatsappMessage[]>([]);
  const [selectedWhatsapp, setSelectedWhatsapp] = useState('');
  const [inboxDraft, setInboxDraft] = useState('');
  const [isLoadingInbox, setIsLoadingInbox] = useState(false);
  const [inboxError, setInboxError] = useState('');
  const [historyConversations, setHistoryConversations] = useState<DbConversation[]>([]);
  const [historyMessages, setHistoryMessages] = useState<DbMessage[]>([]);
  const [historyAudit, setHistoryAudit] = useState<AuditItem[]>([]);
  const [historyWebhookEvents, setHistoryWebhookEvents] = useState<WebhookEventItem[]>([]);
  const [selectedHistoryPhone, setSelectedHistoryPhone] = useState('');
  const [historyPhoneFilter, setHistoryPhoneFilter] = useState('');
  const [historyStatusFilter, setHistoryStatusFilter] = useState<'all' | 'success' | 'error'>('all');
  const [historyLimit, setHistoryLimit] = useState(200);
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);
  const [historyError, setHistoryError] = useState('');
  const [adminToken, setAdminToken] = useState(() => {
    if (typeof window === 'undefined') return '';
    return window.localStorage.getItem(ADMIN_TOKEN_STORAGE_KEY) || '';
  });
  const [adminStatus, setAdminStatus] = useState('');
  const [isLoadingAdmin, setIsLoadingAdmin] = useState(false);
  const [adminTenants, setAdminTenants] = useState<AdminTenant[]>([]);
  const [selectedAdminTenantCode, setSelectedAdminTenantCode] = useState('');
  const [adminTenantIdentifiers, setAdminTenantIdentifiers] = useState<AdminTenantIdentifier[]>([]);
  const [adminTenantProviderConfigs, setAdminTenantProviderConfigs] = useState<AdminTenantProviderConfig[]>([]);
  const [adminCreateName, setAdminCreateName] = useState('');
  const [adminCreateCode, setAdminCreateCode] = useState('');
  const [adminCreateSegment, setAdminCreateSegment] = useState('salao');
  const [adminCreateProvider, setAdminCreateProvider] = useState('trinks');
  const [adminCreateEstablishmentId, setAdminCreateEstablishmentId] = useState('');
  const [adminEditName, setAdminEditName] = useState('');
  const [adminEditSegment, setAdminEditSegment] = useState('');
  const [adminEditProvider, setAdminEditProvider] = useState('trinks');
  const [adminEditEstablishmentId, setAdminEditEstablishmentId] = useState('');
  const [adminEditActive, setAdminEditActive] = useState(true);
  const [adminIdentifierKind, setAdminIdentifierKind] = useState('evolution_instance');
  const [adminIdentifierValue, setAdminIdentifierValue] = useState('');
  const [adminProviderConfigName, setAdminProviderConfigName] = useState('google_calendar');
  const [adminProviderEnabled, setAdminProviderEnabled] = useState(false);
  const [adminProviderConfigJson, setAdminProviderConfigJson] = useState('{}');
  const [adminResolveKind, setAdminResolveKind] = useState('evolution_instance');
  const [adminResolveValue, setAdminResolveValue] = useState('');
  const [adminResolveResult, setAdminResolveResult] = useState('');
  const [adminPrincipal, setAdminPrincipal] = useState<AdminPrincipal | null>(null);
  const [tenantLoginCode, setTenantLoginCode] = useState(() => {
    if (typeof window === 'undefined') return '';
    return window.localStorage.getItem(TENANT_LOGIN_LAST_STORAGE_KEY) || '';
  });
  const [tenantLoginUsername, setTenantLoginUsername] = useState('');
  const [tenantLoginPassword, setTenantLoginPassword] = useState('');
  const [adminTenantUsers, setAdminTenantUsers] = useState<AdminTenantUser[]>([]);
  const [adminCreateUserName, setAdminCreateUserName] = useState('');
  const [adminCreateUserDisplayName, setAdminCreateUserDisplayName] = useState('');
  const [adminCreateUserPassword, setAdminCreateUserPassword] = useState('');
  const [adminCreateUserActive, setAdminCreateUserActive] = useState(true);
  const [adminResetUserId, setAdminResetUserId] = useState('');
  const [adminResetUserPassword, setAdminResetUserPassword] = useState('');
  const [adminResetUserActive, setAdminResetUserActive] = useState(true);
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

  useEffect(() => {
    if (activeSection === 'inbox') {
      loadInbox();
    }
  }, [activeSection]);

  useEffect(() => {
    if (activeSection === 'inbox' && selectedWhatsapp) {
      loadInboxMessages(selectedWhatsapp);
    }
  }, [activeSection, selectedWhatsapp]);

  useEffect(() => {
    if (activeSection === 'history') {
      loadHistory();
    }
  }, [activeSection]);

  useEffect(() => {
    if (activeSection === 'history' && selectedHistoryPhone) {
      loadHistoryMessages(selectedHistoryPhone);
    }
  }, [activeSection, selectedHistoryPhone]);

  useEffect(() => {
    if (activeSection === 'admin' && adminToken) {
      loadAdminTenants(adminToken);
    }
  }, [activeSection, adminToken]);

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
  const whatsappStatusRaw = whatsappStatus?.data || null;
  const whatsappConnectionStatus =
    whatsappStatusRaw && typeof whatsappStatusRaw === 'object'
      ? (whatsappStatusRaw as Record<string, unknown>)?.connectionStatus
      : null;
  const whatsappOwnerJid =
    whatsappStatusRaw && typeof whatsappStatusRaw === 'object'
      ? String((whatsappStatusRaw as Record<string, unknown>)?.ownerJid || '')
      : '';
  const whatsappProfileName =
    whatsappStatusRaw && typeof whatsappStatusRaw === 'object'
      ? String((whatsappStatusRaw as Record<string, unknown>)?.profileName || '')
      : '';
  const whatsappProfilePic =
    whatsappStatusRaw && typeof whatsappStatusRaw === 'object'
      ? String((whatsappStatusRaw as Record<string, unknown>)?.profilePicUrl || '')
      : '';
  const whatsappPhoneNumber = whatsappOwnerJid ? whatsappOwnerJid.split('@')[0] : '';
  const whatsappStatusLabel = whatsappStatus
    ? String(whatsappConnectionStatus || (whatsappStatus.connected ? 'connected' : 'disconnected'))
    : 'desconhecido';
  const whatsappStatusBadgeClass = whatsappStatus
    ? whatsappStatus.connected
      ? 'bg-green-500/20 text-green-200 border-green-400/40'
      : 'bg-red-500/20 text-red-200 border-red-400/40'
    : 'bg-white/10 text-white/70 border-white/10';

  const ensureWhatsappInstance = () => {
    if (!whatsappInstance.trim()) {
      setWhatsappError('Informe o nome da instancia para continuar.');
      return false;
    }
    return true;
  };

  const handleRefreshWhatsappStatus = async () => {
    if (!appointmentService.current || isLoadingWhatsapp || !ensureWhatsappInstance()) return;
    setIsLoadingWhatsapp(true);
    setWhatsappError('');

    try {
      const status = await appointmentService.current.getEvolutionStatus(whatsappInstance.trim());
      setWhatsappStatus(status);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao consultar status do WhatsApp.';
      setWhatsappError(message);
    } finally {
      setIsLoadingWhatsapp(false);
    }
  };

  const handleLoadWhatsappQr = async () => {
    if (!appointmentService.current || isLoadingWhatsapp || !ensureWhatsappInstance()) return;
    setIsLoadingWhatsapp(true);
    setWhatsappError('');

    try {
      await appointmentService.current.createEvolutionInstance(whatsappInstance.trim());
      const qr = await appointmentService.current.getEvolutionQr(whatsappInstance.trim());
      setWhatsappQr(qr);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao carregar QR code do WhatsApp.';
      setWhatsappError(message);
    } finally {
      setIsLoadingWhatsapp(false);
    }
  };

  const handleDisconnectWhatsapp = async () => {
    if (!appointmentService.current || isLoadingWhatsapp || !ensureWhatsappInstance()) return;
    setIsLoadingWhatsapp(true);
    setWhatsappError('');
    try {
      await appointmentService.current.disconnectEvolutionInstance(whatsappInstance.trim());
      setWhatsappStatus(null);
      setWhatsappQr(null);
      setInboxConversations([]);
      setInboxMessages([]);
      setSelectedWhatsapp('');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao desconectar WhatsApp.';
      setWhatsappError(message);
    } finally {
      setIsLoadingWhatsapp(false);
    }
  };

  const loadHistoryMessages = async (phone: string) => {
    if (!appointmentService.current || !phone) return;

    try {
      const messages = await appointmentService.current.getDbMessages(normalizeDigits(phone), historyLimit);
      setHistoryMessages(messages);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao carregar mensagens do historico.';
      setHistoryError(message);
    }
  };

  const loadHistory = async (forcedPhone = '') => {
    if (!appointmentService.current || isLoadingHistory) return;
    setIsLoadingHistory(true);
    setHistoryError('');

    try {
      const phoneCandidate = normalizeDigits(forcedPhone || historyPhoneFilter || selectedHistoryPhone);
      const statusCandidate = historyStatusFilter === 'all' ? '' : historyStatusFilter;

      const [conversations, audit, webhookEvents] = await Promise.all([
        appointmentService.current.getDbConversations(200),
        appointmentService.current.getAppointmentsAudit({
          phone: phoneCandidate || undefined,
          status: statusCandidate || undefined,
          limit: historyLimit,
        }),
        appointmentService.current.getWebhookEvents({
          phone: phoneCandidate || undefined,
          limit: historyLimit,
        }),
      ]);

      setHistoryConversations(conversations);
      setHistoryAudit(audit);
      setHistoryWebhookEvents(webhookEvents);

      if (phoneCandidate) {
        setSelectedHistoryPhone(phoneCandidate);
      } else if (!selectedHistoryPhone && conversations[0]?.phone) {
        setSelectedHistoryPhone(normalizeDigits(conversations[0].phone));
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao carregar historico.';
      setHistoryError(message);
    } finally {
      setIsLoadingHistory(false);
    }
  };

  const handleApplyHistoryFilters = async () => {
    await loadHistory(historyPhoneFilter);
  };

  const handleExportAuditCsv = () => {
    if (!historyAudit.length && !historyWebhookEvents.length) {
      setHistoryError('Nao ha dados para exportar.');
      return;
    }

    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-');
    const workbook = XLSX.utils.book_new();

    if (historyAudit.length) {
      const auditRows = historyAudit.map((item) => {
        const requestReference = toText(item?.requestPayload?.requestReference);
        return {
          ID: item.id ?? '',
          DataHoraRegistro: item.createdAt ? new Date(item.createdAt).toLocaleString('pt-BR') : '',
          Evento: item.eventType ?? '',
          Status: item.status ?? '',
          Cliente: item.clientName ?? '',
          Telefone: item.clientPhone ?? '',
          Servico: item.serviceName ?? '',
          Profissional: item.professionalName ?? '',
          DataAgendamento: item.date ?? '',
          HoraAgendamento: item.time ?? '',
          CodigoTRK: item.confirmationCode ?? '',
          AppointmentId: item.appointmentId ?? '',
          RequestReference: requestReference ?? '',
          Erro: item.errorMessage ?? '',
        };
      });
      const auditSheet = XLSX.utils.json_to_sheet(auditRows);
      auditSheet['!cols'] = [
        { wch: 8 },
        { wch: 20 },
        { wch: 14 },
        { wch: 12 },
        { wch: 24 },
        { wch: 16 },
        { wch: 20 },
        { wch: 20 },
        { wch: 14 },
        { wch: 14 },
        { wch: 14 },
        { wch: 14 },
        { wch: 18 },
        { wch: 36 },
      ];
      XLSX.utils.book_append_sheet(workbook, auditSheet, 'Agendamentos');
    }

    if (historyWebhookEvents.length) {
      const webhookRows = historyWebhookEvents.map((item) => ({
        ID: item.id ?? '',
        DataHoraRecebimento: item.receivedAt ? new Date(item.receivedAt).toLocaleString('pt-BR') : '',
        Evento: item.event ?? '',
        Instancia: item.instanceName ?? '',
        Telefone: item.senderNumber ?? '',
        Nome: item.senderName ?? '',
        MessageId: item.messageId ?? '',
        Tipo: item.messageType ?? '',
        Status: item.status ?? '',
        Motivo: item.reason ?? '',
        Mensagem: item.messageText ?? '',
      }));
      const webhookSheet = XLSX.utils.json_to_sheet(webhookRows);
      webhookSheet['!cols'] = [
        { wch: 8 },
        { wch: 20 },
        { wch: 20 },
        { wch: 18 },
        { wch: 16 },
        { wch: 24 },
        { wch: 22 },
        { wch: 12 },
        { wch: 12 },
        { wch: 20 },
        { wch: 48 },
      ];
      XLSX.utils.book_append_sheet(workbook, webhookSheet, 'Webhook');
    }

    XLSX.writeFile(workbook, `historico-ia-agendamento-${stamp}.xlsx`);
    setHistoryError('');
  };

  const loadInbox = async () => {
    if (!appointmentService.current || isLoadingInbox) return;
    setIsLoadingInbox(true);
    setInboxError('');

    try {
      const conversations = await appointmentService.current.getWhatsappConversations();
      setInboxConversations(conversations);
      if (!selectedWhatsapp && conversations[0]?.phone) {
        setSelectedWhatsapp(conversations[0].phone);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao carregar inbox.';
      setInboxError(message);
    } finally {
      setIsLoadingInbox(false);
    }
  };

  const loadInboxMessages = async (phone: string) => {
    if (!appointmentService.current || isLoadingInbox || !phone) return;
    setIsLoadingInbox(true);
    setInboxError('');

    try {
      const messages = await appointmentService.current.getWhatsappMessages(phone);
      setInboxMessages(messages);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao carregar mensagens.';
      setInboxError(message);
    } finally {
      setIsLoadingInbox(false);
    }
  };

  const handleSendInboxMessage = async () => {
    if (!appointmentService.current || !selectedWhatsapp || !inboxDraft.trim() || isLoadingInbox) return;
    const text = inboxDraft.trim();
    setInboxDraft('');
    setIsLoadingInbox(true);
    setInboxError('');

    try {
      await appointmentService.current.sendWhatsappMessage(selectedWhatsapp, text);
      setInboxMessages((prev) => [...prev, { role: 'assistant', content: text, at: new Date().toISOString() }]);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao enviar mensagem.';
      setInboxError(message);
      setInboxDraft(text);
    } finally {
      setIsLoadingInbox(false);
    }
  };

  const loadAdminSession = async (tokenOverride?: string) => {
    if (!appointmentService.current) return null;
    const tokenToUse = (tokenOverride ?? adminToken).trim();
    if (!tokenToUse) {
      setAdminPrincipal(null);
      return null;
    }

    const payload = await appointmentService.current.getAdminSession(tokenToUse);
    const principal = payload.principal || null;
    setAdminPrincipal(principal);
    return principal;
  };

  const loadAdminTenants = async (tokenOverride?: string) => {
    if (!appointmentService.current || isLoadingAdmin) return;
    const tokenToUse = (tokenOverride ?? adminToken).trim();
    if (!tokenToUse) return;
    setIsLoadingAdmin(true);
    setAdminStatus('');

    try {
      const principal = await loadAdminSession(tokenToUse);
      if (!principal) {
        throw new Error('Sessao admin invalida. Faça login novamente.');
      }

      const tenants = await appointmentService.current.getAdminTenants(tokenToUse, {
        withDetails: true,
        includeInactive: principal.role === 'superadmin',
      });
      setAdminTenants(tenants);

      const nextCode =
        selectedAdminTenantCode && tenants.some((item) => item.code === selectedAdminTenantCode)
          ? selectedAdminTenantCode
          : (tenants[0]?.code || '');

      setSelectedAdminTenantCode(nextCode);
      if (nextCode) {
        await loadAdminTenant(nextCode, tokenToUse);
      } else {
        setAdminTenantIdentifiers([]);
        setAdminTenantProviderConfigs([]);
        setAdminTenantUsers([]);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao carregar tenants.';
      setAdminStatus(message);
    } finally {
      setIsLoadingAdmin(false);
    }
  };

  const loadAdminTenant = async (code: string, tokenOverride?: string) => {
    if (!appointmentService.current || !code) return;
    const tokenToUse = (tokenOverride ?? adminToken).trim();
    if (!tokenToUse) return;
    setIsLoadingAdmin(true);
    setAdminStatus('');
    try {
      const principal = adminPrincipal || (await loadAdminSession(tokenToUse));
      if (!principal) {
        throw new Error('Sessao admin invalida.');
      }
      const payload = await appointmentService.current.getAdminTenant(tokenToUse, code);
      const tenant = payload.tenant;
      setAdminTenantIdentifiers(Array.isArray(payload.identifiers) ? payload.identifiers : []);
      setAdminTenantProviderConfigs(Array.isArray(payload.providerConfigs) ? payload.providerConfigs : []);
      setAdminTenantUsers(Array.isArray(payload.users) ? payload.users : []);

      if (tenant) {
        setAdminEditName(tenant.name || '');
        setAdminEditSegment(tenant.segment || '');
        setAdminEditProvider(tenant.defaultProvider || 'trinks');
        setAdminEditEstablishmentId(
          tenant.establishmentId === null || tenant.establishmentId === undefined ? '' : String(tenant.establishmentId),
        );
        setAdminEditActive(Boolean(tenant.active));
      }

      if (principal.role === 'superadmin') {
        const usersPayload = await appointmentService.current.getAdminTenantUsers(tokenToUse, code);
        setAdminTenantUsers(Array.isArray(usersPayload.users) ? usersPayload.users : []);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao carregar tenant.';
      setAdminStatus(message);
    } finally {
      setIsLoadingAdmin(false);
    }
  };

  const handleSaveAdminToken = async () => {
    const normalized = adminToken.trim();
    if (!normalized) {
      setAdminStatus('Informe o token admin.');
      return;
    }
    try {
      window.localStorage.setItem(ADMIN_TOKEN_STORAGE_KEY, normalized);
      setAdminStatus('Token salvo. Validando acesso...');
      await loadAdminSession(normalized);
      await loadAdminTenants(normalized);
      setAdminStatus('Token validado com sucesso.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao validar token admin.';
      setAdminStatus(message);
    }
  };

  const handleCreateTenant = async () => {
    if (!appointmentService.current || !adminToken || !adminCreateName.trim()) {
      setAdminStatus('Informe pelo menos o nome do tenant.');
      return;
    }
    if (!isSuperAdminSession) {
      setAdminStatus('Apenas superadmin pode criar tenants.');
      return;
    }
    setIsLoadingAdmin(true);
    setAdminStatus('');
    try {
      const response = await appointmentService.current.createAdminTenant(adminToken, {
        code: adminCreateCode.trim() || undefined,
        name: adminCreateName.trim(),
        segment: adminCreateSegment.trim() || undefined,
        defaultProvider: adminCreateProvider,
        establishmentId: adminCreateEstablishmentId.trim() ? Number(adminCreateEstablishmentId.trim()) : undefined,
        active: true,
      });
      const createdCode = response.tenant?.code || '';
      setAdminCreateName('');
      setAdminCreateCode('');
      setAdminCreateSegment('salao');
      setAdminCreateProvider('trinks');
      setAdminCreateEstablishmentId('');
      await loadAdminTenants();
      if (createdCode) {
        setSelectedAdminTenantCode(createdCode);
        await loadAdminTenant(createdCode);
      }
      setAdminStatus('Tenant criado com sucesso.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao criar tenant.';
      setAdminStatus(message);
    } finally {
      setIsLoadingAdmin(false);
    }
  };

  const handleTenantLogin = async () => {
    if (!appointmentService.current) return;
    if (!tenantLoginCode.trim() || !tenantLoginUsername.trim() || !tenantLoginPassword) {
      setAdminStatus('Informe tenant, usuario e senha para entrar.');
      return;
    }

    setIsLoadingAdmin(true);
    setAdminStatus('');
    try {
      const response = await appointmentService.current.loginAdminTenantUser({
        tenantCode: tenantLoginCode.trim(),
        username: tenantLoginUsername.trim(),
        password: tenantLoginPassword,
      });

      const token = (response.token || '').trim();
      if (!token) {
        throw new Error('Login retornou sem token de sessao.');
      }

      setAdminToken(token);
      setTenantLoginPassword('');
      window.localStorage.setItem(ADMIN_TOKEN_STORAGE_KEY, token);
      window.localStorage.setItem(TENANT_LOGIN_LAST_STORAGE_KEY, tenantLoginCode.trim());

      await loadAdminSession(token);
      await loadAdminTenants(token);
      setAdminStatus('Login do tenant realizado com sucesso.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao entrar como tenant.';
      setAdminStatus(message);
    } finally {
      setIsLoadingAdmin(false);
    }
  };

  const handleAdminLogout = async () => {
    if (!appointmentService.current || !adminToken.trim()) return;
    setIsLoadingAdmin(true);
    try {
      await appointmentService.current.logoutAdminSession(adminToken.trim());
    } catch {
      // Ignore backend logout failure and clear local session anyway.
    } finally {
      window.localStorage.removeItem(ADMIN_TOKEN_STORAGE_KEY);
      setAdminToken('');
      setAdminPrincipal(null);
      setAdminTenants([]);
      setSelectedAdminTenantCode('');
      setAdminTenantIdentifiers([]);
      setAdminTenantProviderConfigs([]);
      setAdminTenantUsers([]);
      setAdminStatus('Sessao encerrada.');
      setIsLoadingAdmin(false);
    }
  };

  const handleUpdateTenant = async () => {
    if (!appointmentService.current || !adminToken || !selectedAdminTenantCode) {
      setAdminStatus('Selecione um tenant para atualizar.');
      return;
    }
    setIsLoadingAdmin(true);
    setAdminStatus('');
    try {
      const payload: {
        name?: string;
        segment?: string;
        defaultProvider?: string;
        establishmentId?: number;
        active?: boolean;
      } = {
        name: adminEditName.trim(),
        segment: adminEditSegment.trim(),
      };

      if (isSuperAdminSession) {
        payload.defaultProvider = adminEditProvider;
        payload.establishmentId = adminEditEstablishmentId.trim()
          ? Number(adminEditEstablishmentId.trim())
          : undefined;
        payload.active = adminEditActive;
      }

      await appointmentService.current.updateAdminTenant(adminToken, selectedAdminTenantCode, payload);
      await loadAdminTenants();
      await loadAdminTenant(selectedAdminTenantCode);
      setAdminStatus('Tenant atualizado com sucesso.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao atualizar tenant.';
      setAdminStatus(message);
    } finally {
      setIsLoadingAdmin(false);
    }
  };

  const handleAddTenantIdentifier = async () => {
    if (!appointmentService.current || !adminToken || !selectedAdminTenantCode) {
      setAdminStatus('Selecione um tenant.');
      return;
    }
    if (!adminIdentifierValue.trim()) {
      setAdminStatus('Informe o valor do identificador.');
      return;
    }
    setIsLoadingAdmin(true);
    setAdminStatus('');
    try {
      const response = await appointmentService.current.addAdminTenantIdentifier(
        adminToken,
        selectedAdminTenantCode,
        adminIdentifierKind,
        adminIdentifierValue.trim(),
      );
      setAdminTenantIdentifiers(Array.isArray(response.identifiers) ? response.identifiers : []);
      setAdminIdentifierValue('');
      setAdminStatus('Identificador salvo com sucesso.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao salvar identificador.';
      setAdminStatus(message);
    } finally {
      setIsLoadingAdmin(false);
    }
  };

  const handleSaveProviderConfig = async () => {
    if (!appointmentService.current || !adminToken || !selectedAdminTenantCode) {
      setAdminStatus('Selecione um tenant.');
      return;
    }
    setIsLoadingAdmin(true);
    setAdminStatus('');
    try {
      const config = safeParseKnowledge(adminProviderConfigJson) as Record<string, any>;
      const response = await appointmentService.current.setAdminTenantProviderConfig(
        adminToken,
        selectedAdminTenantCode,
        adminProviderConfigName,
        {
          enabled: adminProviderEnabled,
          config,
        },
      );
      setAdminTenantProviderConfigs(Array.isArray(response.providerConfigs) ? response.providerConfigs : []);
      setAdminStatus('Configuracao de provider salva.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao salvar provider.';
      setAdminStatus(message);
    } finally {
      setIsLoadingAdmin(false);
    }
  };

  const handleResolveTenant = async () => {
    if (!appointmentService.current || !adminToken || !adminResolveValue.trim()) {
      setAdminResolveResult('Informe tipo e valor para resolver.');
      return;
    }
    if (!isSuperAdminSession) {
      setAdminResolveResult('Disponivel apenas para superadmin.');
      return;
    }
    setIsLoadingAdmin(true);
    setAdminResolveResult('');
    try {
      const response = await appointmentService.current.resolveAdminTenant(
        adminToken,
        adminResolveKind,
        adminResolveValue.trim(),
      );
      if (response.found && response.tenant?.code) {
        setAdminResolveResult(`Encontrado: ${response.tenant.name} (${response.tenant.code})`);
        setSelectedAdminTenantCode(response.tenant.code);
        await loadAdminTenant(response.tenant.code);
      } else {
        setAdminResolveResult('Nenhum tenant encontrado para esse identificador.');
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao resolver tenant.';
      setAdminResolveResult(message);
    } finally {
      setIsLoadingAdmin(false);
    }
  };

  const handleCreateTenantUser = async () => {
    if (!appointmentService.current || !adminToken || !selectedAdminTenantCode) {
      setAdminStatus('Selecione um tenant para criar usuario.');
      return;
    }
    if (!adminCreateUserName.trim() || !adminCreateUserPassword.trim()) {
      setAdminStatus('Informe username e senha para o usuario do tenant.');
      return;
    }

    setIsLoadingAdmin(true);
    setAdminStatus('');
    try {
      const response = await appointmentService.current.createAdminTenantUser(
        adminToken,
        selectedAdminTenantCode,
        {
          username: adminCreateUserName.trim(),
          displayName: adminCreateUserDisplayName.trim(),
          password: adminCreateUserPassword,
          active: adminCreateUserActive,
        },
      );
      setAdminTenantUsers(Array.isArray(response.users) ? response.users : []);
      setAdminCreateUserName('');
      setAdminCreateUserDisplayName('');
      setAdminCreateUserPassword('');
      setAdminCreateUserActive(true);
      setAdminStatus('Usuario do tenant criado com sucesso.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao criar usuario do tenant.';
      setAdminStatus(message);
    } finally {
      setIsLoadingAdmin(false);
    }
  };

  const handleResetTenantUser = async () => {
    if (!appointmentService.current || !adminToken || !selectedAdminTenantCode) {
      setAdminStatus('Selecione um tenant para atualizar usuario.');
      return;
    }
    const parsedUserId = Number(adminResetUserId);
    if (!Number.isFinite(parsedUserId) || parsedUserId <= 0) {
      setAdminStatus('Selecione um usuario valido para atualizar.');
      return;
    }
    if (!adminResetUserPassword.trim()) {
      setAdminStatus('Informe a nova senha do usuario.');
      return;
    }

    setIsLoadingAdmin(true);
    setAdminStatus('');
    try {
      const response = await appointmentService.current.updateAdminTenantUser(
        adminToken,
        selectedAdminTenantCode,
        parsedUserId,
        {
          password: adminResetUserPassword,
          active: adminResetUserActive,
        },
      );
      setAdminTenantUsers(Array.isArray(response.users) ? response.users : []);
      setAdminResetUserPassword('');
      setAdminStatus('Usuario atualizado com sucesso.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao atualizar usuario do tenant.';
      setAdminStatus(message);
    } finally {
      setIsLoadingAdmin(false);
    }
  };

  const selectedAdminTenant = adminTenants.find((item) => item.code === selectedAdminTenantCode) || null;
  const isSuperAdminSession = adminPrincipal?.role === 'superadmin';
  const isTenantSession = adminPrincipal?.role === 'tenant';

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
          <button onClick={() => setActiveSection('inbox')} className="hover:text-white cursor-pointer transition-colors">Inbox</button>
          <button onClick={() => setActiveSection('history')} className="hover:text-white cursor-pointer transition-colors">Historico</button>
          <button onClick={() => setActiveSection('admin')} className="hover:text-white cursor-pointer transition-colors">Admin</button>
          <button onClick={() => setActiveSection('contact')} className="hover:text-white cursor-pointer transition-colors">Contato</button>
          <button onClick={() => setActiveSection('knowledge')} className="hover:text-white cursor-pointer transition-colors">Base</button>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 max-w-5xl w-full mx-auto p-4 sm:p-8 flex flex-col gap-6 overflow-y-hidden overflow-x-visible">
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

            {activeSection === 'inbox' && (
              <>
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <h2 className="heading-bold text-lg text-white">Inbox WhatsApp</h2>
                  <button
                    onClick={() => {
                      loadInbox();
                      if (selectedWhatsapp) {
                        loadInboxMessages(selectedWhatsapp);
                      }
                    }}
                    disabled={isLoadingInbox}
                    className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                  >
                    {isLoadingInbox ? 'Atualizando...' : 'Atualizar inbox'}
                  </button>
                </div>
                {inboxError && <p className="text-xs text-red-300">{inboxError}</p>}
                <div className="grid md:grid-cols-[280px_1fr] gap-4">
                  <div className="space-y-2 max-h-[520px] overflow-y-auto pr-1">
                    {!inboxConversations.length && (
                      <p className="text-white/70 text-sm">Nenhuma conversa recebida ainda.</p>
                    )}
                    {inboxConversations.map((item) => (
                      <button
                        key={item.phone}
                        onClick={() => setSelectedWhatsapp(item.phone)}
                        className={`w-full text-left rounded-xl border p-3 transition-colors ${
                          selectedWhatsapp === item.phone
                            ? 'border-brand-blue bg-white/10'
                            : 'border-white/10 bg-white/5 hover:bg-white/10'
                        }`}
                      >
                        <div className="text-sm text-white">
                          {item.name || item.phone}
                        </div>
                        <div className="text-xs text-white/60 truncate">
                          {item.lastRole === 'assistant' ? 'IA: ' : ''}{item.lastMessage || 'Sem mensagens'}
                        </div>
                      </button>
                    ))}
                  </div>
                  <div className="flex flex-col rounded-2xl bg-white/5 border border-white/10 p-4 min-h-[520px]">
                    {!selectedWhatsapp && (
                      <p className="text-white/70 text-sm">Selecione uma conversa para visualizar.</p>
                    )}
                    {selectedWhatsapp && (
                      <>
                        <div className="text-xs text-white/60 mb-3">
                          Conversa com <span className="text-white">{selectedWhatsapp}</span>
                        </div>
                        <div className="flex-1 space-y-3 overflow-y-auto pr-1">
                          {!inboxMessages.length && (
                            <p className="text-white/70 text-sm">Sem mensagens nesta conversa.</p>
                          )}
                          {inboxMessages.map((message, idx) => (
                            <div
                              key={`${message.role}-${idx}`}
                              className={`max-w-[80%] rounded-2xl px-4 py-3 text-sm ${
                                message.role === 'assistant'
                                  ? 'ml-auto bg-brand-blue/20 text-white'
                                  : 'bg-white/10 text-white/90'
                              }`}
                            >
                              <div>{message.content}</div>
                              {message.at && (
                                <div className="text-[10px] text-white/50 mt-1">
                                  {new Date(message.at).toLocaleString('pt-BR')}
                                </div>
                              )}
                            </div>
                          ))}
                        </div>
                        <div className="mt-4 flex items-center gap-2">
                          <input
                            value={inboxDraft}
                            onChange={(e) => setInboxDraft(e.target.value)}
                            placeholder="Responder cliente..."
                            className="flex-1 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                          />
                          <button
                            onClick={handleSendInboxMessage}
                            disabled={isLoadingInbox || !inboxDraft.trim()}
                            className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                          >
                            Enviar
                          </button>
                        </div>
                      </>
                    )}
                  </div>
                </div>
              </>
            )}

            {activeSection === 'history' && (
              <>
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <h2 className="heading-bold text-lg text-white">Historico e Auditoria</h2>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={handleApplyHistoryFilters}
                      disabled={isLoadingHistory}
                      className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                    >
                      {isLoadingHistory ? 'Atualizando...' : 'Atualizar'}
                    </button>
                    <button
                      onClick={handleExportAuditCsv}
                      disabled={isLoadingHistory || (!historyAudit.length && !historyWebhookEvents.length)}
                      className="px-4 py-2 rounded-lg bg-white/10 text-white text-xs uppercase tracking-wider disabled:opacity-50"
                    >
                      Exportar Excel
                    </button>
                  </div>
                </div>

                <div className="grid sm:grid-cols-[1fr_180px_120px] gap-3">
                  <input
                    value={historyPhoneFilter}
                    onChange={(e) => setHistoryPhoneFilter(normalizeDigits(e.target.value))}
                    placeholder="Filtrar por telefone (somente numeros)"
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <select
                    value={historyStatusFilter}
                    onChange={(e) => setHistoryStatusFilter(e.target.value as 'all' | 'success' | 'error')}
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  >
                    <option value="all">Status: Todos</option>
                    <option value="success">Status: Sucesso</option>
                    <option value="error">Status: Erro</option>
                  </select>
                  <input
                    type="number"
                    min={10}
                    max={1000}
                    value={historyLimit}
                    onChange={(e) => {
                      const next = Number(e.target.value || 200);
                      const bounded = Number.isFinite(next) ? Math.min(1000, Math.max(10, next)) : 200;
                      setHistoryLimit(bounded);
                    }}
                    className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                    title="Quantidade maxima de registros"
                  />
                </div>

                {historyError && <p className="text-xs text-red-300">{historyError}</p>}

                <div className="grid lg:grid-cols-[280px_1fr] gap-4">
                  <div className="rounded-2xl bg-white/5 border border-white/10 p-3 max-h-[420px] overflow-y-auto">
                    <p className="text-xs uppercase tracking-widest text-white/60 mb-2">Conversas</p>
                    {!historyConversations.length && (
                      <p className="text-white/70 text-sm">Sem conversas registradas.</p>
                    )}
                    <div className="space-y-2">
                      {historyConversations.map((item) => (
                        <button
                          key={`history-conv-${item.phone}`}
                          onClick={() => {
                            const phone = normalizeDigits(item.phone || '');
                            setSelectedHistoryPhone(phone);
                            setHistoryPhoneFilter(phone);
                            loadHistory(phone);
                          }}
                          className={`w-full text-left rounded-xl border p-3 transition-colors ${
                            selectedHistoryPhone === normalizeDigits(item.phone || '')
                              ? 'border-brand-blue bg-white/10'
                              : 'border-white/10 bg-white/5 hover:bg-white/10'
                          }`}
                        >
                          <div className="text-sm text-white">{item.name || item.phone}</div>
                          <div className="text-[11px] text-white/60 truncate">
                            {item.lastRole === 'assistant' ? 'IA: ' : ''}{item.lastMessage || 'Sem mensagens'}
                          </div>
                        </button>
                      ))}
                    </div>
                  </div>

                  <div className="rounded-2xl bg-white/5 border border-white/10 p-4 min-h-[420px]">
                    <p className="text-xs uppercase tracking-widest text-white/60 mb-2">Mensagens</p>
                    {!selectedHistoryPhone && (
                      <p className="text-white/70 text-sm">Selecione um telefone para visualizar as mensagens.</p>
                    )}
                    {selectedHistoryPhone && (
                      <>
                        <p className="text-xs text-white/60 mb-3">
                          Telefone: <span className="text-white">{selectedHistoryPhone}</span>
                        </p>
                        <div className="space-y-2 max-h-[320px] overflow-y-auto pr-1">
                          {!historyMessages.length && (
                            <p className="text-white/70 text-sm">Sem mensagens para este telefone.</p>
                          )}
                          {historyMessages.map((msg, idx) => (
                            <div
                              key={`history-msg-${msg.id || msg.at || idx}`}
                              className={`rounded-xl px-3 py-2 text-sm ${
                                msg.role === 'assistant' ? 'bg-brand-blue/20 text-white' : 'bg-white/10 text-white/90'
                              }`}
                            >
                              <div>{msg.content}</div>
                              <div className="text-[10px] text-white/50 mt-1">
                                {(msg.at && new Date(msg.at).toLocaleString('pt-BR')) || 'Sem data'}
                                {msg.source ? ` | origem: ${msg.source}` : ''}
                              </div>
                            </div>
                          ))}
                        </div>
                      </>
                    )}
                  </div>
                </div>

                <div className="rounded-2xl bg-white/5 border border-white/10 p-4">
                  <p className="text-xs uppercase tracking-widest text-white/60 mb-3">
                    Auditoria de Agendamentos ({historyAudit.length})
                  </p>
                  <div className="overflow-x-auto">
                    <table className="w-full text-left text-xs text-white/85">
                      <thead className="text-white/60">
                        <tr>
                          <th className="py-2 pr-4">Data</th>
                          <th className="py-2 pr-4">Status</th>
                          <th className="py-2 pr-4">Cliente</th>
                          <th className="py-2 pr-4">Servico</th>
                          <th className="py-2 pr-4">Profissional</th>
                          <th className="py-2 pr-4">Horario</th>
                          <th className="py-2 pr-4">TRK</th>
                          <th className="py-2 pr-4">REQ</th>
                          <th className="py-2 pr-0">Erro</th>
                        </tr>
                      </thead>
                      <tbody>
                        {!historyAudit.length && (
                          <tr>
                            <td colSpan={9} className="py-3 text-white/60">
                              Nenhum registro de auditoria para os filtros atuais.
                            </td>
                          </tr>
                        )}
                        {historyAudit.map((item) => {
                          const requestReference = toText(item?.requestPayload?.requestReference);
                          return (
                            <tr key={`audit-${item.id || `${item.createdAt}-${item.appointmentId}`}`} className="border-t border-white/10">
                              <td className="py-2 pr-4 whitespace-nowrap">
                                {item.createdAt ? new Date(item.createdAt).toLocaleString('pt-BR') : '-'}
                              </td>
                              <td className="py-2 pr-4 uppercase">{item.status || '-'}</td>
                              <td className="py-2 pr-4">{item.clientName || item.clientPhone || '-'}</td>
                              <td className="py-2 pr-4">{item.serviceName || '-'}</td>
                              <td className="py-2 pr-4">{item.professionalName || '-'}</td>
                              <td className="py-2 pr-4 whitespace-nowrap">{item.date || '-'} {item.time || ''}</td>
                              <td className="py-2 pr-4 whitespace-nowrap">{item.confirmationCode || '-'}</td>
                              <td className="py-2 pr-4 whitespace-nowrap">{requestReference || '-'}</td>
                              <td className="py-2 pr-0 text-red-200">{item.errorMessage || '-'}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div className="rounded-2xl bg-white/5 border border-white/10 p-4">
                  <p className="text-xs uppercase tracking-widest text-white/60 mb-3">
                    Eventos do Webhook ({historyWebhookEvents.length})
                  </p>
                  <div className="overflow-x-auto">
                    <table className="w-full text-left text-xs text-white/85">
                      <thead className="text-white/60">
                        <tr>
                          <th className="py-2 pr-4">Data</th>
                          <th className="py-2 pr-4">Telefone</th>
                          <th className="py-2 pr-4">Status</th>
                          <th className="py-2 pr-4">Motivo</th>
                          <th className="py-2 pr-4">Tipo</th>
                          <th className="py-2 pr-0">Mensagem</th>
                        </tr>
                      </thead>
                      <tbody>
                        {!historyWebhookEvents.length && (
                          <tr>
                            <td colSpan={6} className="py-3 text-white/60">
                              Nenhum evento de webhook para os filtros atuais.
                            </td>
                          </tr>
                        )}
                        {historyWebhookEvents.map((item) => (
                          <tr key={`webhook-${item.id || `${item.receivedAt}-${item.messageId}`}`} className="border-t border-white/10">
                            <td className="py-2 pr-4 whitespace-nowrap">
                              {item.receivedAt ? new Date(item.receivedAt).toLocaleString('pt-BR') : '-'}
                            </td>
                            <td className="py-2 pr-4">{item.senderName || item.senderNumber || '-'}</td>
                            <td className="py-2 pr-4 uppercase">{item.status || '-'}</td>
                            <td className="py-2 pr-4">{item.reason || '-'}</td>
                            <td className="py-2 pr-4">{item.messageType || '-'}</td>
                            <td className="py-2 pr-0 max-w-[360px] truncate" title={item.messageText || ''}>
                              {item.messageText || '-'}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </>
            )}

            {activeSection === 'admin' && (
              <>
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <h2 className="heading-bold text-lg text-white">Painel Admin</h2>
                  <button
                    onClick={loadAdminTenants}
                    disabled={isLoadingAdmin || !adminToken.trim()}
                    className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                  >
                    {isLoadingAdmin ? 'Carregando...' : 'Atualizar'}
                  </button>
                </div>

                <div className="rounded-xl bg-white/5 border border-white/10 p-4 space-y-3">
                  <p className="text-xs uppercase tracking-wider text-white/60">Acesso</p>
                  <div className="flex flex-col xl:flex-row xl:flex-wrap gap-2">
                    <input
                      value={adminToken}
                      onChange={(e) => setAdminToken(e.target.value)}
                      placeholder="Cole o x-admin-token"
                      className="min-w-0 xl:flex-1 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                    />
                    <button
                      onClick={handleSaveAdminToken}
                      disabled={isLoadingAdmin}
                      className="w-full xl:w-auto px-4 py-2 rounded-lg bg-brand-green text-black text-xs uppercase tracking-wider disabled:opacity-50"
                    >
                      Salvar token
                    </button>
                    <button
                      onClick={handleAdminLogout}
                      disabled={isLoadingAdmin || !adminToken.trim()}
                      className="w-full xl:w-auto px-4 py-2 rounded-lg bg-white/10 text-white text-xs uppercase tracking-wider disabled:opacity-50"
                    >
                      Sair
                    </button>
                  </div>
                  {adminPrincipal && (
                    <p className="text-xs text-white/80">
                      Sessao atual: <strong>{adminPrincipal.role}</strong>
                      {adminPrincipal.tenantCode ? ` | tenant: ${adminPrincipal.tenantCode}` : ''}
                      {adminPrincipal.username ? ` | usuario: ${adminPrincipal.username}` : ''}
                    </p>
                  )}

                  <div className="pt-3 border-t border-white/10 space-y-2">
                    <p className="text-xs uppercase tracking-wider text-white/60">Login cliente (tenant)</p>
                    <div className="grid md:grid-cols-3 gap-2">
                      <input
                        value={tenantLoginCode}
                        onChange={(e) => setTenantLoginCode(e.target.value)}
                        placeholder="tenant code"
                        className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                      />
                      <input
                        value={tenantLoginUsername}
                        onChange={(e) => setTenantLoginUsername(e.target.value)}
                        placeholder="usuario"
                        className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                      />
                      <input
                        type="password"
                        value={tenantLoginPassword}
                        onChange={(e) => setTenantLoginPassword(e.target.value)}
                        placeholder="senha"
                        className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                      />
                    </div>
                    <button
                      onClick={handleTenantLogin}
                      disabled={isLoadingAdmin}
                      className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                    >
                      Entrar como cliente
                    </button>
                  </div>
                  {adminStatus && <p className="text-xs text-brand-green">{adminStatus}</p>}
                </div>

                <div className="grid xl:grid-cols-[320px_minmax(0,1fr)] gap-4">
                  <div className="rounded-xl bg-white/5 border border-white/10 p-4 space-y-3">
                    <p className="text-xs uppercase tracking-wider text-white/60">Tenants</p>
                    <div className="space-y-2 max-h-[420px] overflow-y-auto pr-1">
                      {!adminTenants.length && (
                        <p className="text-white/60 text-sm">Nenhum tenant carregado.</p>
                      )}
                      {adminTenants.map((tenant) => (
                        <button
                          key={tenant.code}
                          onClick={async () => {
                            setSelectedAdminTenantCode(tenant.code);
                            await loadAdminTenant(tenant.code);
                          }}
                          className={`w-full text-left rounded-lg border px-3 py-2 ${
                            selectedAdminTenantCode === tenant.code
                              ? 'border-brand-blue bg-white/10'
                              : 'border-white/10 bg-white/5 hover:bg-white/10'
                          }`}
                        >
                          <div className="text-sm text-white">{tenant.name}</div>
                          <div className="text-[11px] text-white/60">
                            {tenant.code} | {tenant.defaultProvider || 'trinks'}
                          </div>
                        </button>
                      ))}
                    </div>

                    {isSuperAdminSession && (
                      <div className="pt-3 border-t border-white/10 space-y-2">
                        <p className="text-xs uppercase tracking-wider text-white/60">Novo tenant</p>
                        <input
                          value={adminCreateName}
                          onChange={(e) => setAdminCreateName(e.target.value)}
                          placeholder="Nome"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                        />
                        <input
                          value={adminCreateCode}
                          onChange={(e) => setAdminCreateCode(e.target.value)}
                          placeholder="Code (opcional)"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                        />
                        <input
                          value={adminCreateSegment}
                          onChange={(e) => setAdminCreateSegment(e.target.value)}
                          placeholder="Segmento"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                        />
                        <div className="grid grid-cols-2 gap-2">
                          <select
                            value={adminCreateProvider}
                            onChange={(e) => setAdminCreateProvider(e.target.value)}
                            className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                          >
                            <option value="trinks">trinks</option>
                            <option value="google_calendar">google_calendar</option>
                          </select>
                          <input
                            value={adminCreateEstablishmentId}
                            onChange={(e) => setAdminCreateEstablishmentId(e.target.value)}
                            placeholder="Estab. ID"
                            className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                          />
                        </div>
                        <button
                          onClick={handleCreateTenant}
                          disabled={isLoadingAdmin || !adminToken.trim()}
                          className="w-full px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                        >
                          Criar tenant
                        </button>
                      </div>
                    )}
                  </div>

                  <div className="min-w-0 rounded-xl bg-white/5 border border-white/10 p-4 space-y-4">
                    {!selectedAdminTenant && (
                      <p className="text-white/70 text-sm">Selecione um tenant para editar detalhes.</p>
                    )}

                    {selectedAdminTenant && (
                      <>
                        <div className="space-y-2">
                          <p className="text-xs uppercase tracking-wider text-white/60">Tenant selecionado</p>
                          <div className="grid md:grid-cols-2 gap-2">
                            <input
                              value={adminEditName}
                              onChange={(e) => setAdminEditName(e.target.value)}
                              placeholder="Nome"
                              className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                            />
                            <input
                              value={adminEditSegment}
                              onChange={(e) => setAdminEditSegment(e.target.value)}
                              placeholder="Segmento"
                              className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                            />
                            {isSuperAdminSession && (
                              <select
                                value={adminEditProvider}
                                onChange={(e) => setAdminEditProvider(e.target.value)}
                                className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                              >
                                <option value="trinks">trinks</option>
                                <option value="google_calendar">google_calendar</option>
                              </select>
                            )}
                            {isSuperAdminSession && (
                              <input
                                value={adminEditEstablishmentId}
                                onChange={(e) => setAdminEditEstablishmentId(e.target.value)}
                                placeholder="Establishment ID"
                                className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                              />
                            )}
                          </div>
                          {isSuperAdminSession && (
                            <label className="flex items-center gap-2 text-sm text-white/80">
                              <input
                                type="checkbox"
                                checked={adminEditActive}
                                onChange={(e) => setAdminEditActive(e.target.checked)}
                              />
                              Tenant ativo
                            </label>
                          )}
                          <button
                            onClick={handleUpdateTenant}
                            disabled={isLoadingAdmin || !adminToken.trim()}
                            className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                          >
                            Salvar tenant
                          </button>
                        </div>

                        <div className="space-y-2 pt-3 border-t border-white/10">
                          <p className="text-xs uppercase tracking-wider text-white/60">Identificadores</p>
                          <div className="flex flex-wrap gap-2">
                            {adminTenantIdentifiers.map((identifier) => (
                              <span key={`${identifier.kind}-${identifier.id}`} className="text-xs rounded-full px-3 py-1 bg-white/10 text-white/80">
                                {identifier.kind}: {identifier.value}
                              </span>
                            ))}
                            {!adminTenantIdentifiers.length && (
                              <span className="text-xs text-white/60">Nenhum identificador.</span>
                            )}
                          </div>
                          <div className="grid gap-2 lg:grid-cols-[200px_minmax(0,1fr)_auto]">
                            <select
                              value={adminIdentifierKind}
                              onChange={(e) => setAdminIdentifierKind(e.target.value)}
                              className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                            >
                              <option value="evolution_instance">evolution_instance</option>
                              <option value="evolution_number">evolution_number</option>
                              <option value="domain">domain</option>
                              <option value="api_key">api_key</option>
                              <option value="custom">custom</option>
                            </select>
                            <input
                              value={adminIdentifierValue}
                              onChange={(e) => setAdminIdentifierValue(e.target.value)}
                              placeholder="Valor do identificador"
                              className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                            />
                            <button
                              onClick={handleAddTenantIdentifier}
                              disabled={isLoadingAdmin || !adminToken.trim()}
                              className="px-4 py-2 rounded-lg bg-brand-green text-black text-xs uppercase tracking-wider disabled:opacity-50"
                            >
                              Adicionar
                            </button>
                          </div>
                        </div>

                        <div className="space-y-2 pt-3 border-t border-white/10">
                          <p className="text-xs uppercase tracking-wider text-white/60">Provider config</p>
                          <div className="flex flex-wrap gap-2">
                            {adminTenantProviderConfigs.map((config) => (
                              <span key={`${config.provider}-${config.id}`} className="text-xs rounded-full px-3 py-1 bg-white/10 text-white/80">
                                {config.provider}: {config.enabled ? 'on' : 'off'}
                              </span>
                            ))}
                          </div>
                          <div className="grid gap-2 sm:grid-cols-2">
                            <select
                              value={adminProviderConfigName}
                              onChange={(e) => setAdminProviderConfigName(e.target.value)}
                              className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                            >
                              <option value="trinks">trinks</option>
                              <option value="google_calendar">google_calendar</option>
                            </select>
                            <label className="flex items-center gap-2 text-sm text-white/80">
                              <input
                                type="checkbox"
                                checked={adminProviderEnabled}
                                onChange={(e) => setAdminProviderEnabled(e.target.checked)}
                              />
                              habilitado
                            </label>
                          </div>
                          <textarea
                            value={adminProviderConfigJson}
                            onChange={(e) => setAdminProviderConfigJson(e.target.value)}
                            className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm min-h-24 font-mono"
                          />
                          <button
                            onClick={handleSaveProviderConfig}
                            disabled={isLoadingAdmin || !adminToken.trim()}
                            className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                          >
                            Salvar provider
                          </button>
                        </div>

                        {isSuperAdminSession && (
                          <div className="space-y-2 pt-3 border-t border-white/10">
                            <p className="text-xs uppercase tracking-wider text-white/60">Acesso de usuarios do tenant</p>
                            <div className="flex flex-wrap gap-2">
                              {adminTenantUsers.map((user) => (
                                <span key={`tenant-user-${user.id}`} className="text-xs rounded-full px-3 py-1 bg-white/10 text-white/80">
                                  {user.username} ({user.active ? 'ativo' : 'inativo'})
                                </span>
                              ))}
                              {!adminTenantUsers.length && (
                                <span className="text-xs text-white/60">Nenhum usuario neste tenant.</span>
                              )}
                            </div>

                            <div className="grid md:grid-cols-2 gap-2">
                              <input
                                value={adminCreateUserName}
                                onChange={(e) => setAdminCreateUserName(e.target.value)}
                                placeholder="username (ex: leopoldina)"
                                className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                              />
                              <input
                                value={adminCreateUserDisplayName}
                                onChange={(e) => setAdminCreateUserDisplayName(e.target.value)}
                                placeholder="nome exibicao"
                                className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                              />
                              <input
                                type="password"
                                value={adminCreateUserPassword}
                                onChange={(e) => setAdminCreateUserPassword(e.target.value)}
                                placeholder="senha inicial"
                                className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                              />
                              <label className="flex items-center gap-2 text-sm text-white/80">
                                <input
                                  type="checkbox"
                                  checked={adminCreateUserActive}
                                  onChange={(e) => setAdminCreateUserActive(e.target.checked)}
                                />
                                usuario ativo
                              </label>
                            </div>
                            <button
                              onClick={handleCreateTenantUser}
                              disabled={isLoadingAdmin || !adminToken.trim()}
                              className="px-4 py-2 rounded-lg bg-brand-green text-black text-xs uppercase tracking-wider disabled:opacity-50"
                            >
                              Criar usuario do tenant
                            </button>

                            <div className="grid gap-2 pt-2 lg:grid-cols-[180px_minmax(0,1fr)_auto]">
                              <select
                                value={adminResetUserId}
                                onChange={(e) => {
                                  const selected = adminTenantUsers.find((item) => String(item.id) === e.target.value);
                                  setAdminResetUserId(e.target.value);
                                  setAdminResetUserActive(Boolean(selected?.active));
                                }}
                                className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                              >
                                <option value="">Selecionar usuario</option>
                                {adminTenantUsers.map((user) => (
                                  <option key={`opt-user-${user.id}`} value={String(user.id)}>
                                    {user.username}
                                  </option>
                                ))}
                              </select>
                              <input
                                type="password"
                                value={adminResetUserPassword}
                                onChange={(e) => setAdminResetUserPassword(e.target.value)}
                                placeholder="nova senha"
                                className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                              />
                              <button
                                onClick={handleResetTenantUser}
                                disabled={isLoadingAdmin || !adminToken.trim()}
                                className="px-4 py-2 rounded-lg bg-white/10 text-white text-xs uppercase tracking-wider disabled:opacity-50"
                              >
                                Atualizar usuario
                              </button>
                            </div>
                            <label className="flex items-center gap-2 text-sm text-white/80">
                              <input
                                type="checkbox"
                                checked={adminResetUserActive}
                                onChange={(e) => setAdminResetUserActive(e.target.checked)}
                              />
                              manter usuario ativo
                            </label>
                          </div>
                        )}

                        {isSuperAdminSession && (
                          <div className="space-y-2 pt-3 border-t border-white/10">
                          <p className="text-xs uppercase tracking-wider text-white/60">Resolver por identificador</p>
                          <div className="grid gap-2 lg:grid-cols-[220px_minmax(0,1fr)_auto]">
                            <select
                              value={adminResolveKind}
                              onChange={(e) => setAdminResolveKind(e.target.value)}
                              className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                            >
                              <option value="evolution_instance">evolution_instance</option>
                              <option value="evolution_number">evolution_number</option>
                              <option value="domain">domain</option>
                              <option value="api_key">api_key</option>
                              <option value="custom">custom</option>
                            </select>
                            <input
                              value={adminResolveValue}
                              onChange={(e) => setAdminResolveValue(e.target.value)}
                              placeholder="Valor"
                              className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                            />
                            <button
                              onClick={handleResolveTenant}
                              disabled={isLoadingAdmin || !adminToken.trim()}
                              className="px-4 py-2 rounded-lg bg-white/10 text-white text-xs uppercase tracking-wider disabled:opacity-50"
                            >
                              Resolver
                            </button>
                          </div>
                          {adminResolveResult && <p className="text-xs text-white/80">{adminResolveResult}</p>}
                          </div>
                        )}
                      </>
                    )}
                  </div>
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
                <div className="mt-6 rounded-2xl bg-white/5 border border-white/10 p-4 space-y-4">
                  <div className="flex items-center justify-between gap-3 flex-wrap">
                    <div className="flex items-center gap-2">
                      <Phone className="w-4 h-4 text-brand-blue" />
                      <h3 className="text-sm uppercase tracking-widest text-white">WhatsApp</h3>
                    </div>
                    <div className="flex items-center gap-2">
                      <button
                        onClick={handleRefreshWhatsappStatus}
                        disabled={isLoadingWhatsapp}
                        className="px-3 py-2 rounded-lg bg-white/10 text-white text-xs uppercase tracking-wider disabled:opacity-50"
                      >
                        Atualizar status
                      </button>
                      <button
                        onClick={handleLoadWhatsappQr}
                        disabled={isLoadingWhatsapp}
                        className="px-3 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                      >
                        {isLoadingWhatsapp ? 'Carregando...' : 'Gerar QR'}
                      </button>
                      <button
                        onClick={handleDisconnectWhatsapp}
                        disabled={isLoadingWhatsapp || !whatsappStatus?.connected}
                        className="px-3 py-2 rounded-lg bg-red-600 text-white text-xs uppercase tracking-wider disabled:opacity-50"
                      >
                        {isLoadingWhatsapp ? 'Desconectando...' : 'Desconectar'}
                      </button>
                    </div>
                  </div>
                  <div className="grid sm:grid-cols-2 gap-3">
                    <input
                      value={whatsappInstance}
                      onChange={(e) => setWhatsappInstance(e.target.value)}
                      placeholder="Nome da instancia (ex: ia-agendamento)"
                      className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                    />
                    <div className="rounded-md bg-[#0f1731] border border-white/15 text-white/80 px-3 py-2 text-sm flex items-center">
                      Status:
                      <span
                        className={`ml-2 px-2 py-0.5 rounded-full border text-[11px] uppercase tracking-wider ${whatsappStatusBadgeClass}`}
                      >
                        {whatsappStatusLabel}
                      </span>
                    </div>
                  </div>
                  {whatsappError && <p className="text-xs text-red-300">{whatsappError}</p>}
                  {(whatsappPhoneNumber || whatsappProfileName || whatsappProfilePic) && (
                    <div className="rounded-xl bg-white/5 border border-white/10 p-4 space-y-3">
                      <div className="flex items-center gap-3">
                        {whatsappProfilePic ? (
                          <img
                            src={whatsappProfilePic}
                            alt="Perfil WhatsApp"
                            className="w-12 h-12 rounded-full border border-white/20 object-cover"
                          />
                        ) : (
                          <div className="w-12 h-12 rounded-full border border-white/20 bg-white/5" />
                        )}
                        <div>
                          <p className="text-xs uppercase tracking-wider text-white/60">Conta conectada</p>
                          {whatsappProfileName && <p className="text-sm text-white">{whatsappProfileName}</p>}
                          {whatsappPhoneNumber && <p className="text-xs text-white/70">{whatsappPhoneNumber}</p>}
                        </div>
                      </div>
                    </div>
                  )}
                  <div className="rounded-xl bg-white/5 border border-white/10 p-4 flex items-center justify-center min-h-[220px]">
                    {whatsappQr?.qr?.qrDataUrl ? (
                      <img src={whatsappQr.qr.qrDataUrl} alt="QR Code WhatsApp" className="w-56 h-auto" />
                    ) : (
                      <p className="text-xs text-white/60 text-center">
                        Clique em Gerar QR para conectar o WhatsApp da unidade.
                      </p>
                    )}
                  </div>
                  {whatsappQr?.qr?.pairingCode && (
                    <p className="text-xs text-white/70">
                      Pairing code: <span className="text-white/90">{whatsappQr.qr.pairingCode}</span>
                    </p>
                  )}
                  <p className="text-[11px] text-white/50">
                    O QR expira em poucos minutos. Se falhar, gere um novo e tente novamente.
                  </p>
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
          <button
            onClick={() => setActiveSection('history')}
            className="px-4 py-2 rounded-full glass label-micro text-white/60 hover:text-white hover:border-white/30 transition-all flex items-center gap-2"
          >
            <Calendar className="w-3 h-3 text-white/70" /> Historico
          </button>
          <button
            onClick={() => setActiveSection('admin')}
            className="px-4 py-2 rounded-full glass label-micro text-white/60 hover:text-white hover:border-white/30 transition-all flex items-center gap-2"
          >
            <Sparkles className="w-3 h-3 text-white/70" /> Admin
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
