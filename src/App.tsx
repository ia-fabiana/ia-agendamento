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

type TopSection = 'chat' | 'services' | 'salon' | 'contact' | 'whatsapp' | 'knowledge' | 'inbox' | 'history' | 'admin' | 'crm';
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

type CrmSettings = {
  crmReturnEnabled?: boolean;
  crmMode?: 'beta' | 'manual' | 'automatic';
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
  priority?: 'low' | 'medium' | 'high';
  notes?: string;
};

type CrmCategoryRule = {
  id?: number;
  categoryKey: string;
  categoryName: string;
  opportunityTrackingEnabled?: boolean;
  opportunityDaysWithoutReturn?: number | null;
  opportunityPriority?: 'low' | 'medium' | 'high';
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
};

type CrmFlowItem = {
  id?: number;
  clientId?: number | null;
  clientName?: string;
  phone?: string;
  originServiceName?: string;
  originCategoryName?: string;
  lastVisitAt?: string;
  lastProfessionalName?: string;
  lastProfessionalActive?: boolean | null;
  flowStatus?: string;
  currentStep?: number;
  stopReason?: string;
  lastMessageSentAt?: string;
  convertedAt?: string;
};

type CrmFlowEvent = {
  id?: number;
  flowId?: number;
  eventType?: string;
  step?: number | null;
  messagePreview?: string;
  messageSent?: string;
  replySummary?: string;
  bookingId?: number | null;
  metadata?: Record<string, unknown>;
  createdAt?: string;
};

type CrmOpportunityItem = {
  id?: number;
  clientId?: number | null;
  clientName?: string;
  phone?: string;
  categoryName?: string;
  sourceServiceName?: string;
  lastRelevantVisitAt?: string;
  daysWithoutReturn?: number | null;
  lastProfessionalName?: string;
  lastProfessionalActive?: boolean | null;
  opportunityStatus?: string;
  priority?: 'low' | 'medium' | 'high';
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
  flowCandidates?: Array<Record<string, any>>;
  opportunityCandidates?: Array<Record<string, any>>;
  skipped?: Array<Record<string, any>>;
};

const initialKnowledge = {
  identity: {
    brandName: 'Fabiana Luxury Salon',
    toneGuide: 'sofisticado, acolhedor, objetivo',
    toneOptions: ['sofisticado', 'acolhedor', 'objetivo'],
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
  marketing: {
    enabled: false,
    actions: [],
  },
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

function mergeCrmSettings(input?: CrmSettings | null): CrmSettings {
  return {
    ...defaultCrmSettings,
    ...(input || {}),
    betaTestPhones: Array.isArray(input?.betaTestPhones) ? input?.betaTestPhones : defaultCrmSettings.betaTestPhones,
  };
}

function mergeCategoryRulesWithCatalog(
  catalog: CrmCatalogService[],
  rules: CrmCategoryRule[],
): CrmCategoryRule[] {
  const map = new Map<string, CrmCategoryRule>();
  (Array.isArray(rules) ? rules : []).forEach((rule) => {
    const key = String(rule?.categoryKey || '').trim();
    if (!key) return;
    map.set(key, {
      categoryKey: key,
      categoryName: String(rule?.categoryName || '').trim(),
      opportunityTrackingEnabled: Boolean(rule?.opportunityTrackingEnabled),
      opportunityDaysWithoutReturn: rule?.opportunityDaysWithoutReturn ?? null,
      opportunityPriority: rule?.opportunityPriority || 'medium',
      allowManualCampaign: rule?.allowManualCampaign ?? true,
      suggestedMessageTemplate: String(rule?.suggestedMessageTemplate || ''),
      notes: String(rule?.notes || ''),
      id: rule?.id,
    });
  });

  (Array.isArray(catalog) ? catalog : []).forEach((service) => {
    const categoryKey = String(service?.categoryKey || '').trim();
    const categoryName = String(service?.categoryName || '').trim();
    if (!categoryKey || !categoryName || map.has(categoryKey)) return;
    map.set(categoryKey, {
      categoryKey,
      categoryName,
      opportunityTrackingEnabled: false,
      opportunityDaysWithoutReturn: null,
      opportunityPriority: 'medium',
      allowManualCampaign: true,
      suggestedMessageTemplate: '',
      notes: '',
    });
  });

  return [...map.values()].sort((a, b) => a.categoryName.localeCompare(b.categoryName, 'pt-BR'));
}

function extractEvolutionInstanceFromIdentifiers(identifiers: AdminTenantIdentifier[]) {
  const list = Array.isArray(identifiers) ? identifiers : [];
  const found = list.find((item) => String(item?.kind || '').trim().toLowerCase() === 'evolution_instance');
  return String(found?.value || '').trim();
}

const ADMIN_TOKEN_STORAGE_KEY = 'ia_agendamento_admin_token';
const TENANT_LOGIN_LAST_STORAGE_KEY = 'ia_agendamento_tenant_last_login';
const TONE_GUIDE_OPTIONS = [
  'sofisticado',
  'acolhedor',
  'objetivo',
  'consultivo',
  'premium',
  'amigavel',
  'direto',
  'calmo',
];

const defaultCrmSettings: CrmSettings = {
  crmReturnEnabled: false,
  crmMode: 'beta',
  bookingMaxDaysAhead: 60,
  messageSendingWindowStart: '09:00',
  messageSendingWindowEnd: '19:00',
  messageDailyLimit: 20,
  stopFlowOnAnyFutureBooking: true,
  maxSteps: 3,
  humanHandoffEnabled: true,
  humanHandoffClientNumber: '',
  humanHandoffInternalNumber: '',
  humanHandoffMessageTemplate:
    'Se preferir, nosso atendimento humano segue com voce pelo numero {{human_number}}.',
  humanHandoffSendInternalSummary: true,
  humanHandoffPauseAi: true,
  opportunityTrackingEnabled: true,
  allowOnlyWhitelistedPhonesInBeta: false,
  betaTestPhones: [],
};

function normalizeToneToken(value: string) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase();
}

function splitToneValues(value: unknown) {
  if (Array.isArray(value)) {
    return value
      .map((item) => String(item || '').trim())
      .filter(Boolean);
  }
  return String(value || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
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
  const [whatsappInstance, setWhatsappInstance] = useState('');
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
  const [adminProviderConfigName, setAdminProviderConfigName] = useState('trinks');
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
  const [crmSettingsDraft, setCrmSettingsDraft] = useState<CrmSettings>(defaultCrmSettings);
  const [crmServiceCatalog, setCrmServiceCatalog] = useState<CrmCatalogService[]>([]);
  const [crmCategoryRules, setCrmCategoryRules] = useState<CrmCategoryRule[]>([]);
  const [crmBlocks, setCrmBlocks] = useState<CrmClientBlock[]>([]);
  const [crmFlows, setCrmFlows] = useState<CrmFlowItem[]>([]);
  const [crmOpportunities, setCrmOpportunities] = useState<CrmOpportunityItem[]>([]);
  const [crmDashboard, setCrmDashboard] = useState<CrmDashboard | null>(null);
  const [crmPreview, setCrmPreview] = useState<CrmPreviewResult | null>(null);
  const [crmStatus, setCrmStatus] = useState('');
  const [isLoadingCrm, setIsLoadingCrm] = useState(false);
  const [expandedFlowId, setExpandedFlowId] = useState<number | null>(null);
  const [crmFlowEvents, setCrmFlowEvents] = useState<Record<number, CrmFlowEvent[]>>({});
  const [crmFlowActionLoading, setCrmFlowActionLoading] = useState<number | null>(null);
  const [crmPreviewLookbackDays, setCrmPreviewLookbackDays] = useState(365);
  const [crmBlockPhone, setCrmBlockPhone] = useState('');
  const [crmBlockClientName, setCrmBlockClientName] = useState('');
  const [crmBlockReason, setCrmBlockReason] = useState('manual_block');
  const [crmBlockNotes, setCrmBlockNotes] = useState('');
  const [uploadingMarketingIndex, setUploadingMarketingIndex] = useState<number | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const appointmentService = useRef<AppointmentService | null>(null);

  const resolveKnowledgeTenantScopeCode = () =>
    adminPrincipal?.role === 'superadmin' ? selectedAdminTenantCode.trim() : '';

  const resolveCrmTenantScopeCode = () =>
    adminPrincipal?.role === 'superadmin'
      ? selectedAdminTenantCode.trim()
      : String(adminPrincipal?.tenantCode || selectedAdminTenantCode || '').trim();

  useEffect(() => {
    appointmentService.current = new AppointmentService();
  }, []);

  useEffect(() => {
    const loadKnowledge = async () => {
      if (!appointmentService.current) return;
      setIsLoadingKnowledge(true);
      try {
        const tenantScopeCode = resolveKnowledgeTenantScopeCode();
        const knowledge = await appointmentService.current.getKnowledge(adminToken, tenantScopeCode);
        setKnowledgeJson(JSON.stringify(knowledge, null, 2));
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Erro ao carregar base de conhecimento.';
        setKnowledgeStatus(message);
      } finally {
        setIsLoadingKnowledge(false);
      }
    };

    loadKnowledge();
  }, [adminPrincipal?.role, adminToken, selectedAdminTenantCode]);

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
    if (activeSection === 'crm') {
      loadCrmData();
    }
  }, [activeSection, adminToken, selectedAdminTenantCode, adminPrincipal?.role, adminPrincipal?.tenantCode]);

  useEffect(() => {
    const token = adminToken.trim();
    if (!token) {
      setAdminPrincipal(null);
      return;
    }
    loadAdminTenants(token);
  }, [adminToken]);

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
        {
          tenantCode: sessionTenantCode,
        },
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
      const tenantScopeCode = resolveKnowledgeTenantScopeCode();
      const saved = await appointmentService.current.saveKnowledge(parsed, adminToken, tenantScopeCode);
      setKnowledgeJson(JSON.stringify(saved, null, 2));
      setKnowledgeStatus('Base de conhecimento salva com sucesso.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao salvar base de conhecimento.';
      setKnowledgeStatus(message);
    } finally {
      setIsSavingKnowledge(false);
    }
  };

  const handleUploadMarketingImage = async (actionIndex: number, file: File | null) => {
    if (!appointmentService.current || !file) return;
    if (!adminToken.trim()) {
      setKnowledgeStatus('Faça login no Admin para enviar imagem de MKT.');
      return;
    }

    setUploadingMarketingIndex(actionIndex);
    setKnowledgeStatus('Enviando imagem de marketing...');
    try {
      const tenantScopeCode = resolveKnowledgeTenantScopeCode();
      const uploadedUrl = await appointmentService.current.uploadMarketingImage(file, adminToken, tenantScopeCode);

      updateKnowledge((draft) => {
        const currentMarketing =
          draft.marketing && typeof draft.marketing === 'object' ? draft.marketing : {};
        const currentActions = Array.isArray(currentMarketing.actions) ? currentMarketing.actions : [];
        currentActions[actionIndex] = { ...(currentActions[actionIndex] || {}), mediaUrl: uploadedUrl };
        draft.marketing = { ...currentMarketing, actions: currentActions };
      });
      setKnowledgeStatus('Imagem enviada com sucesso. Clique em Salvar para persistir.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao enviar imagem de marketing.';
      setKnowledgeStatus(message);
    } finally {
      setUploadingMarketingIndex(null);
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
      setKnowledgeStatus('JSON invalido na aba Marketing. Corrija o JSON para continuar editando pelos formulários.');
    }
  };

  const services = Array.isArray(knowledgeObject?.services) ? knowledgeObject.services : [];
  const faq = Array.isArray(knowledgeObject?.faq) ? knowledgeObject.faq : [];
  const marketingConfig =
    knowledgeObject?.marketing && typeof knowledgeObject.marketing === 'object'
      ? (knowledgeObject.marketing as Record<string, any>)
      : {};
  const marketingActions = Array.isArray(marketingConfig?.actions) ? marketingConfig.actions : [];
  const marketingEnabled = marketingConfig?.enabled === true;
  const paymentMethods = Array.isArray(knowledgeObject?.business?.paymentMethods)
    ? knowledgeObject.business.paymentMethods
    : [];
  const toneOptionMap = new Map(TONE_GUIDE_OPTIONS.map((item) => [normalizeToneToken(item), item]));
  const identityObject =
    knowledgeObject?.identity && typeof knowledgeObject.identity === 'object' && !Array.isArray(knowledgeObject.identity)
      ? (knowledgeObject.identity as Record<string, any>)
      : {};
  const persistedToneOptions = splitToneValues(identityObject?.toneOptions).filter((item) =>
    toneOptionMap.has(normalizeToneToken(item)),
  );
  const legacyToneValues = splitToneValues(identityObject?.toneGuide);
  const selectedToneOptions =
    persistedToneOptions.length > 0
      ? persistedToneOptions.map((item) => toneOptionMap.get(normalizeToneToken(item)) || item)
      : legacyToneValues
          .filter((item) => toneOptionMap.has(normalizeToneToken(item)))
          .map((item) => toneOptionMap.get(normalizeToneToken(item)) || item);
  const selectedToneKeys = new Set(selectedToneOptions.map((item) => normalizeToneToken(item)));
  const customToneValues = splitToneValues(identityObject?.toneCustom);
  const legacyCustomToneValues = legacyToneValues.filter((item) => !toneOptionMap.has(normalizeToneToken(item)));
  const toneCustomText = (customToneValues.length > 0 ? customToneValues : legacyCustomToneValues).join(', ');

  const persistToneConfiguration = (nextToneOptions: string[], nextCustomText = '') => {
    const nextOptionKeys = new Set(nextToneOptions.map((item) => normalizeToneToken(item)));
    const normalizedOptions = TONE_GUIDE_OPTIONS.filter((option) => nextOptionKeys.has(normalizeToneToken(option)));
    const normalizedCustomValues = splitToneValues(nextCustomText).filter(
      (item) => !toneOptionMap.has(normalizeToneToken(item)),
    );
    const nextToneGuide = [...normalizedOptions, ...normalizedCustomValues].join(', ');

    updateKnowledge((draft) => {
      const currentIdentity =
        draft.identity && typeof draft.identity === 'object' && !Array.isArray(draft.identity)
          ? draft.identity
          : {};
      draft.identity = {
        ...currentIdentity,
        toneOptions: normalizedOptions,
        toneCustom: normalizedCustomValues.join(', '),
        toneGuide: nextToneGuide,
      };
    });
  };

  const handleToggleToneOption = (option: string) => {
    const optionKey = normalizeToneToken(option);
    const nextOptions = selectedToneOptions.filter((item) => normalizeToneToken(item) !== optionKey);
    if (!selectedToneKeys.has(optionKey)) {
      nextOptions.push(option);
    }
    persistToneConfiguration(nextOptions, toneCustomText);
  };

  const renderToneSelector = () => (
    <div className="rounded-md bg-[#0f1731] border border-white/15 px-3 py-2 text-sm space-y-2">
      <p className="text-[11px] uppercase tracking-wider text-white/60">Tom das conversas (multipla escolha)</p>
      <div className="flex flex-wrap gap-2">
        {TONE_GUIDE_OPTIONS.map((option) => {
          const checked = selectedToneKeys.has(normalizeToneToken(option));
          return (
            <button
              key={`tone-${option}`}
              type="button"
              onClick={() => handleToggleToneOption(option)}
              className={`px-2 py-1 rounded-full border text-xs ${
                checked
                  ? 'border-brand-blue bg-brand-blue/20 text-brand-blue'
                  : 'border-white/20 bg-white/5 text-white/80 hover:bg-white/10'
              }`}
            >
              {option}
            </button>
          );
        })}
      </div>
      <input
        value={toneCustomText}
        onChange={(e) => persistToneConfiguration(selectedToneOptions, e.target.value)}
        placeholder="Tom personalizado opcional (separado por virgula)"
        className="w-full rounded-md bg-[#0b132b] border border-white/10 text-white/90 px-3 py-2 text-xs"
      />
    </div>
  );
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
      const instanceName = whatsappInstance.trim();

      // Best-effort cleanup before creating a fresh QR. Some WhatsApp devices
      // fail to pair if an older session is still half-open on Evolution.
      try {
        await appointmentService.current.disconnectEvolutionInstance(instanceName);
      } catch {
        // Ignore cleanup failures and continue with a fresh connect attempt.
      }

      await appointmentService.current.createEvolutionInstance(instanceName);
      const qr = await appointmentService.current.getEvolutionQr(instanceName);
      setWhatsappQr(qr);

      const status = await appointmentService.current.getEvolutionStatus(instanceName);
      setWhatsappStatus(status);

      const hasQrImage = Boolean(qr?.qr?.qrDataUrl);
      const hasPairingCode = Boolean(qr?.qr?.pairingCode);
      if (!hasQrImage && !hasPairingCode) {
        setWhatsappError('QR indisponivel no momento. Clique em Gerar QR novamente em alguns segundos.');
      }
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

  const renderWhatsappConnectionBlock = () => (
    <div className="rounded-2xl bg-white/5 border border-white/10 p-4 space-y-4">
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
          disabled={isTenantSession}
          className={`w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm ${
            isTenantSession ? 'opacity-70 cursor-not-allowed' : ''
          }`}
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
      {isTenantSession && (
        <p className="text-[11px] text-white/60">
          Instancia Evolution definida automaticamente pelo tenant logado.
        </p>
      )}
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
  );

  const loadHistoryMessages = async (phone: string) => {
    if (!appointmentService.current || !phone) return;

    try {
      const messages = await appointmentService.current.getDbMessages(
        normalizeDigits(phone),
        historyLimit,
        adminToken,
      );
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
        appointmentService.current.getDbConversations(200, adminToken),
        appointmentService.current.getAppointmentsAudit({
          phone: phoneCandidate || undefined,
          status: statusCandidate || undefined,
          limit: historyLimit,
        }, adminToken),
        appointmentService.current.getWebhookEvents({
          phone: phoneCandidate || undefined,
          limit: historyLimit,
        }, adminToken),
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
      const conversations = await appointmentService.current.getWhatsappConversationsWithAuth(adminToken);
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
      const messages = await appointmentService.current.getWhatsappMessagesWithAuth(phone, adminToken);
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

  const loadCrmData = async () => {
    if (!appointmentService.current || !adminToken.trim()) return;
    const tenantCode = resolveCrmTenantScopeCode();
    if (!tenantCode) {
      setCrmStatus('Selecione um tenant para operar o CRM.');
      return;
    }

    setIsLoadingCrm(true);
    setCrmStatus('');
    try {
      const [
        settingsPayload,
        catalogPayload,
        categoryPayload,
        blocksPayload,
        flowsPayload,
        opportunitiesPayload,
        dashboardPayload,
      ] = await Promise.all([
        appointmentService.current.getTenantCrmSettings(adminToken, tenantCode),
        appointmentService.current.getTenantCrmServiceCatalog(adminToken, tenantCode),
        appointmentService.current.getTenantCrmCategoryRules(adminToken, tenantCode),
        appointmentService.current.getTenantCrmBlocks(adminToken, tenantCode),
        appointmentService.current.getTenantCrmFlows(adminToken, tenantCode, { limit: 100 }),
        appointmentService.current.getTenantCrmOpportunities(adminToken, tenantCode, { limit: 100 }),
        appointmentService.current.getTenantCrmDashboard(adminToken, tenantCode),
      ]);

      const catalog = Array.isArray(catalogPayload.data) ? catalogPayload.data : [];
      const categories = mergeCategoryRulesWithCatalog(
        catalog,
        Array.isArray(categoryPayload.rules) ? categoryPayload.rules : [],
      );

      setCrmSettingsDraft(mergeCrmSettings(settingsPayload.settings));
      setCrmServiceCatalog(catalog);
      setCrmCategoryRules(categories);
      setCrmBlocks(Array.isArray(blocksPayload.blocks) ? blocksPayload.blocks : []);
      setCrmFlows(Array.isArray(flowsPayload.data) ? flowsPayload.data : []);
      setCrmOpportunities(Array.isArray(opportunitiesPayload.data) ? opportunitiesPayload.data : []);
      setCrmDashboard(dashboardPayload.dashboard || null);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao carregar CRM.';
      setCrmStatus(message);
    } finally {
      setIsLoadingCrm(false);
    }
  };

  const handleSaveCrmSettings = async () => {
    if (!appointmentService.current || !adminToken.trim()) return;
    const tenantCode = resolveCrmTenantScopeCode();
    if (!tenantCode) {
      setCrmStatus('Selecione um tenant para salvar configuracoes do CRM.');
      return;
    }

    setIsLoadingCrm(true);
    setCrmStatus('');
    try {
      const response = await appointmentService.current.saveTenantCrmSettings(
        adminToken,
        tenantCode,
        crmSettingsDraft,
      );
      setCrmSettingsDraft(mergeCrmSettings(response.settings));
      setCrmStatus('Configuracoes do CRM salvas com sucesso.');
      const dashboardPayload = await appointmentService.current.getTenantCrmDashboard(adminToken, tenantCode);
      setCrmDashboard(dashboardPayload.dashboard || null);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao salvar configuracoes do CRM.';
      setCrmStatus(message);
    } finally {
      setIsLoadingCrm(false);
    }
  };

  const handleServiceRuleChange = (serviceKey: string, patch: Partial<CrmServiceRule>) => {
    setCrmServiceCatalog((current) =>
      current.map((item) => {
        if (item.serviceKey !== serviceKey) return item;
        const nextRule: CrmServiceRule = {
          serviceKey: item.serviceKey,
          serviceName: item.serviceName,
          categoryKey: item.categoryKey,
          categoryName: item.categoryName,
          active: false,
          useDefaultFlow: true,
          priority: 'medium',
          ...(item.rule || {}),
          ...patch,
        };
        return { ...item, rule: nextRule };
      }),
    );
  };

  const handleSaveCrmServiceRules = async () => {
    if (!appointmentService.current || !adminToken.trim()) return;
    const tenantCode = resolveCrmTenantScopeCode();
    if (!tenantCode) {
      setCrmStatus('Selecione um tenant para salvar regras por servico.');
      return;
    }

    setIsLoadingCrm(true);
    setCrmStatus('');
    try {
      const rules = crmServiceCatalog
        .map((item) => ({
          serviceKey: item.serviceKey,
          serviceName: item.serviceName,
          categoryKey: item.categoryKey,
          categoryName: item.categoryName,
          active: Boolean(item.rule?.active),
          returnDays: item.rule?.returnDays ?? null,
          useDefaultFlow: item.rule?.useDefaultFlow ?? true,
          step1DelayDays: item.rule?.step1DelayDays ?? null,
          step1MessageTemplate: item.rule?.step1MessageTemplate || '',
          step2DelayDays: item.rule?.step2DelayDays ?? null,
          step2MessageTemplate: item.rule?.step2MessageTemplate || '',
          step3DelayDays: item.rule?.step3DelayDays ?? null,
          step3MessageTemplate: item.rule?.step3MessageTemplate || '',
          priority: item.rule?.priority || 'medium',
          notes: item.rule?.notes || '',
        }))
        .filter((item) => item.active || item.returnDays || item.step1MessageTemplate || item.notes);

      await appointmentService.current.saveTenantCrmServiceRules(adminToken, tenantCode, rules);
      await loadCrmData();
      setCrmStatus('Regras por servico salvas com sucesso.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao salvar regras por servico.';
      setCrmStatus(message);
    } finally {
      setIsLoadingCrm(false);
    }
  };

  const handleCategoryRuleChange = (categoryKey: string, patch: Partial<CrmCategoryRule>) => {
    setCrmCategoryRules((current) =>
      current.map((item) =>
        item.categoryKey === categoryKey
          ? {
              ...item,
              ...patch,
            }
          : item,
      ),
    );
  };

  const handleSaveCrmCategoryRules = async () => {
    if (!appointmentService.current || !adminToken.trim()) return;
    const tenantCode = resolveCrmTenantScopeCode();
    if (!tenantCode) {
      setCrmStatus('Selecione um tenant para salvar regras por categoria.');
      return;
    }

    setIsLoadingCrm(true);
    setCrmStatus('');
    try {
      const rules = crmCategoryRules.filter(
        (item) =>
          item.opportunityTrackingEnabled ||
          item.opportunityDaysWithoutReturn ||
          item.suggestedMessageTemplate ||
          item.notes,
      );
      await appointmentService.current.saveTenantCrmCategoryRules(adminToken, tenantCode, rules);
      await loadCrmData();
      setCrmStatus('Regras por categoria salvas com sucesso.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao salvar regras por categoria.';
      setCrmStatus(message);
    } finally {
      setIsLoadingCrm(false);
    }
  };

  const handleSaveCrmBlock = async () => {
    if (!appointmentService.current || !adminToken.trim()) return;
    const tenantCode = resolveCrmTenantScopeCode();
    if (!tenantCode) {
      setCrmStatus('Selecione um tenant para cadastrar bloqueio.');
      return;
    }
    if (!crmBlockPhone.trim()) {
      setCrmStatus('Informe o telefone da cliente para bloquear.');
      return;
    }

    setIsLoadingCrm(true);
    setCrmStatus('');
    try {
      const response = await appointmentService.current.saveTenantCrmBlock(adminToken, tenantCode, {
        phone: crmBlockPhone,
        clientName: crmBlockClientName,
        isBlocked: true,
        blockReason: crmBlockReason,
        blockNotes: crmBlockNotes,
      });
      setCrmBlocks(Array.isArray(response.blocks) ? response.blocks : []);
      setCrmBlockPhone('');
      setCrmBlockClientName('');
      setCrmBlockReason('manual_block');
      setCrmBlockNotes('');
      setCrmStatus('Cliente bloqueada no CRM.');
      const dashboardPayload = await appointmentService.current.getTenantCrmDashboard(adminToken, tenantCode);
      setCrmDashboard(dashboardPayload.dashboard || null);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao salvar bloqueio.';
      setCrmStatus(message);
    } finally {
      setIsLoadingCrm(false);
    }
  };

  const handleRunCrmPreview = async (materialize = false) => {
    if (!appointmentService.current || !adminToken.trim()) return;
    const tenantCode = resolveCrmTenantScopeCode();
    if (!tenantCode) {
      setCrmStatus('Selecione um tenant para rodar o preview do CRM.');
      return;
    }

    setIsLoadingCrm(true);
    setCrmStatus('');
    try {
      const response = await appointmentService.current.runTenantCrmPreview(adminToken, tenantCode, {
        lookbackDays: crmPreviewLookbackDays,
        materialize,
        limit: 250,
      });
      setCrmPreview(response.preview || null);
      setCrmDashboard(response.dashboard || null);
      if (materialize) {
        await loadCrmData();
        setCrmStatus('Preview materializado no CRM beta/manual.');
      } else {
        setCrmStatus('Preview do CRM gerado com sucesso.');
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao rodar preview do CRM.';
      setCrmStatus(message);
    } finally {
      setIsLoadingCrm(false);
    }
  };

  const handleApproveCrmFlow = async (flowId: number) => {
    if (!appointmentService.current || !adminToken.trim()) return;
    const tenantCode = resolveCrmTenantScopeCode();
    if (!tenantCode || !flowId) return;
    setCrmFlowActionLoading(flowId);
    setCrmStatus('');
    try {
      const response = await appointmentService.current.approveCrmFlow(adminToken, tenantCode, flowId);
      setCrmStatus(`Etapa 1 enviada: ${response.messageSent || 'ok'}`);
      await loadCrmData();
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao aprovar fluxo.';
      setCrmStatus(message);
    } finally {
      setCrmFlowActionLoading(null);
    }
  };

  const handleStopCrmFlow = async (flowId: number) => {
    if (!appointmentService.current || !adminToken.trim()) return;
    const tenantCode = resolveCrmTenantScopeCode();
    if (!tenantCode || !flowId) return;
    setCrmFlowActionLoading(flowId);
    setCrmStatus('');
    try {
      await appointmentService.current.stopCrmFlow(adminToken, tenantCode, flowId);
      setCrmStatus('Fluxo encerrado.');
      await loadCrmData();
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao encerrar fluxo.';
      setCrmStatus(message);
    } finally {
      setCrmFlowActionLoading(null);
    }
  };

  const handleToggleCrmFlowEvents = async (flowId: number) => {
    if (expandedFlowId === flowId) {
      setExpandedFlowId(null);
      return;
    }
    setExpandedFlowId(flowId);
    if (crmFlowEvents[flowId]) return;
    if (!appointmentService.current || !adminToken.trim()) return;
    const tenantCode = resolveCrmTenantScopeCode();
    if (!tenantCode) return;
    try {
      const response = await appointmentService.current.getCrmFlowEvents(adminToken, tenantCode, flowId);
      setCrmFlowEvents((prev) => ({ ...prev, [flowId]: response.events || [] }));
    } catch {
      setCrmFlowEvents((prev) => ({ ...prev, [flowId]: [] }));
    }
  };

  const selectedAdminTenant = adminTenants.find((item) => item.code === selectedAdminTenantCode) || null;
  const isSuperAdminSession = adminPrincipal?.role === 'superadmin';
  const isTenantSession = adminPrincipal?.role === 'tenant';
  const sessionTenantName = String(adminPrincipal?.tenantName || selectedAdminTenant?.name || '').trim();
  const sessionTenantCode = String(adminPrincipal?.tenantCode || selectedAdminTenant?.code || '').trim();
  const isAuthenticated = Boolean(adminPrincipal);
  const showSuperAdminEntry =
    typeof window !== 'undefined' && new URLSearchParams(window.location.search).get('admin') === '1';
  const tenantEvolutionInstance = extractEvolutionInstanceFromIdentifiers(adminTenantIdentifiers);

  useEffect(() => {
    if (!isAuthenticated) {
      setWhatsappInstance('');
      setWhatsappStatus(null);
      setWhatsappQr(null);
      setWhatsappError('');
      return;
    }

    const fallbackTenantInstance = String(sessionTenantCode || selectedAdminTenantCode || '').trim();
    const nextInstance = String(tenantEvolutionInstance || fallbackTenantInstance).trim();
    if (!nextInstance) {
      return;
    }

    if (whatsappInstance.trim() !== nextInstance) {
      setWhatsappInstance(nextInstance);
      setWhatsappStatus(null);
      setWhatsappQr(null);
      setWhatsappError('');
    }
  }, [
    isAuthenticated,
    tenantEvolutionInstance,
    sessionTenantCode,
    selectedAdminTenantCode,
    whatsappInstance,
  ]);

  if (!isAuthenticated) {
    return (
      <div className="min-h-screen flex flex-col luxury-gradient">
        <header className="p-6 border-bottom border-white/5 flex items-center justify-between sticky top-0 z-10 glass">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-full bg-brand-purple/20 flex items-center justify-center border border-brand-purple/30">
              <Sparkles className="w-5 h-5 text-brand-purple" />
            </div>
            <div>
              <h1 className="heading-bold text-xl text-white/90">IA.AGENDAMENTO</h1>
              <p className="label-micro text-brand-blue">Acesso Seguro</p>
            </div>
          </div>
        </header>

        <main className="flex-1 max-w-3xl w-full mx-auto p-4 sm:p-8">
          <div className="glass rounded-2xl p-5 sm:p-6 space-y-6">
            <div>
              <h2 className="heading-bold text-lg text-white">Entrar na plataforma</h2>
              <p className="text-sm text-white/70 mt-1">
                Cliente entra com tenant, usuario e senha. Superadmin pode entrar por token.
              </p>
            </div>

            <div className="space-y-2">
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

            {showSuperAdminEntry && (
              <div className="pt-4 border-t border-white/10 space-y-2">
                <p className="text-xs uppercase tracking-wider text-white/60">Acesso superadmin</p>
                <div className="flex flex-col sm:flex-row gap-2">
                  <input
                    value={adminToken}
                    onChange={(e) => setAdminToken(e.target.value)}
                    placeholder="Cole o x-admin-token"
                    className="min-w-0 flex-1 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                  />
                  <button
                    onClick={handleSaveAdminToken}
                    disabled={isLoadingAdmin}
                    className="px-4 py-2 rounded-lg bg-brand-green text-black text-xs uppercase tracking-wider disabled:opacity-50"
                  >
                    Entrar com token
                  </button>
                </div>
              </div>
            )}

            {adminStatus && <p className="text-xs text-brand-green">{adminStatus}</p>}
          </div>
        </main>
      </div>
    );
  }

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
          {isSuperAdminSession && (
            <button onClick={() => setActiveSection('admin')} className="hover:text-white cursor-pointer transition-colors">Admin</button>
          )}
          <button onClick={() => setActiveSection('crm')} className="hover:text-white cursor-pointer transition-colors">CRM</button>
          <button onClick={() => setActiveSection('knowledge')} className="hover:text-white cursor-pointer transition-colors">Marketing</button>
          <button onClick={() => setActiveSection('contact')} className="hover:text-white cursor-pointer transition-colors">FAQ</button>
          <button onClick={() => setActiveSection('whatsapp')} className="hover:text-white cursor-pointer transition-colors">Conexao WhatsApp</button>
          <button onClick={() => setActiveSection('history')} className="hover:text-white cursor-pointer transition-colors">Historico</button>
          <button onClick={() => setActiveSection('inbox')} className="hover:text-white cursor-pointer transition-colors">Inbox</button>
          <button onClick={() => setActiveSection('salon')} className="hover:text-white cursor-pointer transition-colors">Estabelecimento</button>
          <button onClick={() => setActiveSection('services')} className="hover:text-white cursor-pointer transition-colors">Servicos</button>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 max-w-5xl w-full mx-auto p-4 sm:p-8 flex flex-col gap-6 overflow-y-hidden overflow-x-visible">
        <div className="glass rounded-xl p-3 sm:p-4 border border-white/10 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
          <div className="min-w-0">
            <p className="text-xs uppercase tracking-wider text-white/60">Sessao atual</p>
            <p className="text-sm text-white/90 break-words">
              {isSuperAdminSession
                ? 'Superadmin'
                : `Tenant: ${sessionTenantName || sessionTenantCode || 'nao identificado'}`}
              {adminPrincipal?.username ? ` | usuario: ${adminPrincipal.username}` : ''}
            </p>
          </div>
          <button
            onClick={handleAdminLogout}
            disabled={isLoadingAdmin}
            className="w-full sm:w-auto px-4 py-2 rounded-lg bg-white/10 text-white text-xs uppercase tracking-wider disabled:opacity-50"
          >
            Sair
          </button>
        </div>

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

            {activeSection === 'crm' && (
              <>
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <div>
                    <h2 className="heading-bold text-lg text-white">CRM de Retorno</h2>
                    <p className="text-sm text-white/65 mt-1">
                      Tenant em operacao: {resolveCrmTenantScopeCode() || 'selecione um tenant'}
                    </p>
                  </div>
                  <div className="flex items-center gap-2 flex-wrap">
                    <input
                      type="number"
                      min={30}
                      max={730}
                      value={crmPreviewLookbackDays}
                      onChange={(e) => setCrmPreviewLookbackDays(Number(e.target.value || 365))}
                      className="w-28 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                      title="Dias para olhar para tras no preview"
                    />
                    <button
                      onClick={() => handleRunCrmPreview(false)}
                      disabled={isLoadingCrm || !adminToken.trim()}
                      className="px-4 py-2 rounded-lg bg-brand-green text-black text-xs uppercase tracking-wider disabled:opacity-50"
                    >
                      Preview beta
                    </button>
                    <button
                      onClick={() => handleRunCrmPreview(true)}
                      disabled={isLoadingCrm || !adminToken.trim()}
                      className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                    >
                      Materializar
                    </button>
                    <button
                      onClick={loadCrmData}
                      disabled={isLoadingCrm || !adminToken.trim()}
                      className="px-4 py-2 rounded-lg bg-white/10 text-white text-xs uppercase tracking-wider disabled:opacity-50"
                    >
                      {isLoadingCrm ? 'Atualizando...' : 'Atualizar CRM'}
                    </button>
                    <button
                      onClick={handleSaveCrmSettings}
                      disabled={isLoadingCrm || !adminToken.trim()}
                      className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                    >
                      Salvar base CRM
                    </button>
                  </div>
                </div>

                {crmStatus && <p className="text-xs text-brand-green">{crmStatus}</p>}

                {crmPreview && (
                  <div className="rounded-xl bg-white/5 border border-white/10 p-4 space-y-4">
                    <div className="flex items-center justify-between gap-3 flex-wrap">
                      <div>
                        <p className="text-sm uppercase tracking-wider text-white/70">Preview / Beta Manual</p>
                        <p className="text-xs text-white/55 mt-1">
                          Gerado em {crmPreview.generatedAt ? new Date(crmPreview.generatedAt).toLocaleString('pt-BR') : '-'} | modo {crmPreview.crmMode || '-'}
                        </p>
                      </div>
                      <div className="text-xs text-white/60">
                        {crmPreview.materialize ? 'Preview materializado' : 'Apenas simulacao'}
                      </div>
                    </div>
                    <div className="grid md:grid-cols-4 gap-3">
                      {[
                        ['Candidatas a fluxo', crmPreview.summary?.flowCandidates || 0],
                        ['Oportunidades', crmPreview.summary?.opportunityCandidates || 0],
                        ['Puladas', crmPreview.summary?.skipped || 0],
                        ['Linhas auditadas', crmPreview.summary?.auditedRows || 0],
                      ].map(([label, value]) => (
                        <div key={String(label)} className="rounded-lg bg-white/5 border border-white/10 p-3">
                          <p className="text-[11px] uppercase tracking-wider text-white/55">{label}</p>
                          <p className="heading-bold text-xl text-white mt-2">{value}</p>
                        </div>
                      ))}
                    </div>
                    <div className="grid xl:grid-cols-2 gap-4">
                      <div className="rounded-lg bg-white/5 border border-white/10 p-3">
                        <p className="text-xs uppercase tracking-wider text-white/60 mb-2">
                          Candidatas ({Array.isArray(crmPreview.flowCandidates) ? crmPreview.flowCandidates.length : 0})
                        </p>
                        <div className="space-y-2 max-h-[260px] overflow-y-auto pr-1">
                          {!crmPreview.flowCandidates?.length && <p className="text-sm text-white/60">Nenhuma candidata a fluxo neste preview.</p>}
                          {crmPreview.flowCandidates?.map((item, idx) => (
                            <div key={`crm-preview-flow-${idx}`} className="rounded-lg bg-white/5 border border-white/10 p-3">
                              <div className="text-sm text-white">{String(item.clientName || item.phone || 'Cliente')}</div>
                              <div className="text-xs text-white/60">
                                {String(item.originServiceName || '-')} | {String(item.originCategoryName || '-')}
                              </div>
                              <div className="text-xs text-white/75 mt-1">
                                Ultima visita: {String(item.lastVisitAt || '-')} | Profissional: {String(item.lastProfessionalName || 'nao informado')}
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                      <div className="rounded-lg bg-white/5 border border-white/10 p-3">
                        <p className="text-xs uppercase tracking-wider text-white/60 mb-2">
                          Oportunidades ({Array.isArray(crmPreview.opportunityCandidates) ? crmPreview.opportunityCandidates.length : 0})
                        </p>
                        <div className="space-y-2 max-h-[260px] overflow-y-auto pr-1">
                          {!crmPreview.opportunityCandidates?.length && <p className="text-sm text-white/60">Nenhuma oportunidade neste preview.</p>}
                          {crmPreview.opportunityCandidates?.map((item, idx) => (
                            <div key={`crm-preview-opportunity-${idx}`} className="rounded-lg bg-white/5 border border-white/10 p-3">
                              <div className="text-sm text-white">{String(item.clientName || item.phone || 'Cliente')}</div>
                              <div className="text-xs text-white/60">
                                {String(item.categoryName || '-')} | origem {String(item.sourceServiceName || '-')}
                              </div>
                              <div className="text-xs text-white/75 mt-1">
                                Sem retorno ha {String(item.daysWithoutReturn || 0)} dias
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  </div>
                )}

                <div className="grid md:grid-cols-4 gap-3">
                  {[
                    ['Servicos configurados', crmDashboard?.totals?.configuredServices || 0],
                    ['Regras ativas', crmDashboard?.totals?.activeServiceRules || 0],
                    ['Clientes bloqueadas', crmDashboard?.totals?.blockedClients || 0],
                    ['Oportunidades', crmDashboard?.totals?.opportunitiesTotal || 0],
                  ].map(([label, value]) => (
                    <div key={String(label)} className="rounded-xl bg-white/5 border border-white/10 p-4">
                      <p className="text-xs uppercase tracking-wider text-white/60">{label}</p>
                      <p className="heading-bold text-2xl text-white mt-2">{value}</p>
                    </div>
                  ))}
                </div>

                <div className="rounded-xl bg-white/5 border border-white/10 p-4 space-y-4">
                  <div className="flex items-center justify-between gap-3 flex-wrap">
                    <h3 className="text-sm uppercase tracking-wider text-white/70">Configuracao Global</h3>
                    <button
                      onClick={handleSaveCrmSettings}
                      disabled={isLoadingCrm || !adminToken.trim()}
                      className="px-4 py-2 rounded-lg bg-brand-green text-black text-xs uppercase tracking-wider disabled:opacity-50"
                    >
                      Salvar configuracoes
                    </button>
                  </div>
                  <div className="grid lg:grid-cols-3 gap-3">
                    <label className="flex items-center gap-2 text-sm text-white/80">
                      <input
                        type="checkbox"
                        checked={Boolean(crmSettingsDraft.crmReturnEnabled)}
                        onChange={(e) => setCrmSettingsDraft((current) => ({ ...current, crmReturnEnabled: e.target.checked }))}
                      />
                      CRM de retorno ativo
                    </label>
                    <label className="flex items-center gap-2 text-sm text-white/80">
                      <input
                        type="checkbox"
                        checked={Boolean(crmSettingsDraft.stopFlowOnAnyFutureBooking)}
                        onChange={(e) => setCrmSettingsDraft((current) => ({ ...current, stopFlowOnAnyFutureBooking: e.target.checked }))}
                      />
                      Para se houver qualquer agendamento
                    </label>
                    <label className="flex items-center gap-2 text-sm text-white/80">
                      <input
                        type="checkbox"
                        checked={Boolean(crmSettingsDraft.humanHandoffEnabled)}
                        onChange={(e) => setCrmSettingsDraft((current) => ({ ...current, humanHandoffEnabled: e.target.checked }))}
                      />
                      Handoff humano ativo
                    </label>
                    <select
                      value={crmSettingsDraft.crmMode || 'beta'}
                      onChange={(e) =>
                        setCrmSettingsDraft((current) => ({
                          ...current,
                          crmMode: e.target.value as 'beta' | 'manual' | 'automatic',
                        }))
                      }
                      className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                    >
                      <option value="beta">Modo beta</option>
                      <option value="manual">Aprovacao manual</option>
                      <option value="automatic">Automatico</option>
                    </select>
                    <input
                      type="number"
                      min={1}
                      max={365}
                      value={crmSettingsDraft.bookingMaxDaysAhead ?? 60}
                      onChange={(e) =>
                        setCrmSettingsDraft((current) => ({
                          ...current,
                          bookingMaxDaysAhead: Number(e.target.value || 60),
                        }))
                      }
                      placeholder="Dias maximos para agendar"
                      className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                    />
                    <input
                      type="number"
                      min={1}
                      max={1000}
                      value={crmSettingsDraft.messageDailyLimit ?? 20}
                      onChange={(e) =>
                        setCrmSettingsDraft((current) => ({
                          ...current,
                          messageDailyLimit: Number(e.target.value || 20),
                        }))
                      }
                      placeholder="Limite diario"
                      className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                    />
                    <input
                      value={crmSettingsDraft.messageSendingWindowStart || ''}
                      onChange={(e) =>
                        setCrmSettingsDraft((current) => ({ ...current, messageSendingWindowStart: e.target.value }))
                      }
                      placeholder="Inicio envio (09:00)"
                      className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                    />
                    <input
                      value={crmSettingsDraft.messageSendingWindowEnd || ''}
                      onChange={(e) =>
                        setCrmSettingsDraft((current) => ({ ...current, messageSendingWindowEnd: e.target.value }))
                      }
                      placeholder="Fim envio (19:00)"
                      className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                    />
                    <input
                      value={crmSettingsDraft.humanHandoffClientNumber || ''}
                      onChange={(e) =>
                        setCrmSettingsDraft((current) => ({ ...current, humanHandoffClientNumber: normalizeDigits(e.target.value) }))
                      }
                      placeholder="Numero humano para a cliente"
                      className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                    />
                    <input
                      value={crmSettingsDraft.humanHandoffInternalNumber || ''}
                      onChange={(e) =>
                        setCrmSettingsDraft((current) => ({ ...current, humanHandoffInternalNumber: normalizeDigits(e.target.value) }))
                      }
                      placeholder="Numero interno para resumo"
                      className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                    />
                  </div>
                  <textarea
                    value={crmSettingsDraft.humanHandoffMessageTemplate || ''}
                    onChange={(e) =>
                      setCrmSettingsDraft((current) => ({ ...current, humanHandoffMessageTemplate: e.target.value }))
                    }
                    placeholder="Mensagem padrao de handoff humano"
                    className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm min-h-20"
                  />
                </div>

                <div className="rounded-xl bg-white/5 border border-white/10 p-4 space-y-4">
                  <div className="flex items-center justify-between gap-3 flex-wrap">
                    <h3 className="text-sm uppercase tracking-wider text-white/70">Regras Por Servico</h3>
                    <button
                      onClick={handleSaveCrmServiceRules}
                      disabled={isLoadingCrm || !adminToken.trim()}
                      className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                    >
                      Salvar servicos
                    </button>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-left text-xs text-white/85">
                      <thead className="text-white/60">
                        <tr>
                          <th className="py-2 pr-3">Ativo</th>
                          <th className="py-2 pr-3">Servico</th>
                          <th className="py-2 pr-3">Categoria</th>
                          <th className="py-2 pr-3">Retorno</th>
                          <th className="py-2 pr-3">Etapa 1</th>
                          <th className="py-2 pr-3">Etapa 2</th>
                          <th className="py-2 pr-3">Etapa 3</th>
                          <th className="py-2 pr-0">Obs.</th>
                        </tr>
                      </thead>
                      <tbody>
                        {!crmServiceCatalog.length && (
                          <tr>
                            <td colSpan={8} className="py-3 text-white/60">Nenhum servico carregado do Trinks ainda.</td>
                          </tr>
                        )}
                        {crmServiceCatalog.map((item) => (
                          <tr key={item.serviceKey} className="border-t border-white/10 align-top">
                            <td className="py-2 pr-3">
                              <input
                                type="checkbox"
                                checked={Boolean(item.rule?.active)}
                                onChange={(e) => handleServiceRuleChange(item.serviceKey, { active: e.target.checked })}
                              />
                            </td>
                            <td className="py-2 pr-3">
                              <div className="font-medium">{item.serviceName}</div>
                              <div className="text-[10px] text-white/50">
                                {item.durationMinutes ? `${item.durationMinutes} min` : 'duracao ?'}
                                {typeof item.price === 'number' ? ` | R$ ${item.price}` : ''}
                              </div>
                            </td>
                            <td className="py-2 pr-3">{item.categoryName || '-'}</td>
                            <td className="py-2 pr-3">
                              <input
                                type="number"
                                min={0}
                                value={item.rule?.returnDays ?? ''}
                                onChange={(e) =>
                                  handleServiceRuleChange(item.serviceKey, {
                                    returnDays: e.target.value ? Number(e.target.value) : null,
                                  })
                                }
                                className="w-20 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-2 py-1 text-xs"
                              />
                            </td>
                            <td className="py-2 pr-3">
                              <textarea
                                value={item.rule?.step1MessageTemplate || ''}
                                onChange={(e) => handleServiceRuleChange(item.serviceKey, { step1MessageTemplate: e.target.value })}
                                className="w-44 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-2 py-1 text-xs min-h-16"
                              />
                            </td>
                            <td className="py-2 pr-3">
                              <textarea
                                value={item.rule?.step2MessageTemplate || ''}
                                onChange={(e) => handleServiceRuleChange(item.serviceKey, { step2MessageTemplate: e.target.value })}
                                className="w-44 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-2 py-1 text-xs min-h-16"
                              />
                            </td>
                            <td className="py-2 pr-3">
                              <textarea
                                value={item.rule?.step3MessageTemplate || ''}
                                onChange={(e) => handleServiceRuleChange(item.serviceKey, { step3MessageTemplate: e.target.value })}
                                className="w-44 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-2 py-1 text-xs min-h-16"
                              />
                            </td>
                            <td className="py-2 pr-0">
                              <textarea
                                value={item.rule?.notes || ''}
                                onChange={(e) => handleServiceRuleChange(item.serviceKey, { notes: e.target.value })}
                                className="w-40 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-2 py-1 text-xs min-h-16"
                              />
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div className="grid xl:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)] gap-4">
                  <div className="rounded-xl bg-white/5 border border-white/10 p-4 space-y-4">
                    <div className="flex items-center justify-between gap-3 flex-wrap">
                      <h3 className="text-sm uppercase tracking-wider text-white/70">Oportunidades Por Categoria</h3>
                      <button
                        onClick={handleSaveCrmCategoryRules}
                        disabled={isLoadingCrm || !adminToken.trim()}
                        className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                      >
                        Salvar categorias
                      </button>
                    </div>
                    <div className="overflow-x-auto">
                      <table className="w-full text-left text-xs text-white/85">
                        <thead className="text-white/60">
                          <tr>
                            <th className="py-2 pr-3">Ativa</th>
                            <th className="py-2 pr-3">Categoria</th>
                            <th className="py-2 pr-3">Dias</th>
                            <th className="py-2 pr-3">Prioridade</th>
                            <th className="py-2 pr-0">Mensagem sugerida</th>
                          </tr>
                        </thead>
                        <tbody>
                          {!crmCategoryRules.length && (
                            <tr>
                              <td colSpan={5} className="py-3 text-white/60">Nenhuma categoria derivada do catalogo ainda.</td>
                            </tr>
                          )}
                          {crmCategoryRules.map((item) => (
                            <tr key={item.categoryKey} className="border-t border-white/10 align-top">
                              <td className="py-2 pr-3">
                                <input
                                  type="checkbox"
                                  checked={Boolean(item.opportunityTrackingEnabled)}
                                  onChange={(e) =>
                                    handleCategoryRuleChange(item.categoryKey, { opportunityTrackingEnabled: e.target.checked })
                                  }
                                />
                              </td>
                              <td className="py-2 pr-3">{item.categoryName}</td>
                              <td className="py-2 pr-3">
                                <input
                                  type="number"
                                  min={0}
                                  value={item.opportunityDaysWithoutReturn ?? ''}
                                  onChange={(e) =>
                                    handleCategoryRuleChange(item.categoryKey, {
                                      opportunityDaysWithoutReturn: e.target.value ? Number(e.target.value) : null,
                                    })
                                  }
                                  className="w-20 rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-2 py-1 text-xs"
                                />
                              </td>
                              <td className="py-2 pr-3">
                                <select
                                  value={item.opportunityPriority || 'medium'}
                                  onChange={(e) =>
                                    handleCategoryRuleChange(item.categoryKey, {
                                      opportunityPriority: e.target.value as 'low' | 'medium' | 'high',
                                    })
                                  }
                                  className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-2 py-1 text-xs"
                                >
                                  <option value="low">Baixa</option>
                                  <option value="medium">Media</option>
                                  <option value="high">Alta</option>
                                </select>
                              </td>
                              <td className="py-2 pr-0">
                                <textarea
                                  value={item.suggestedMessageTemplate || ''}
                                  onChange={(e) =>
                                    handleCategoryRuleChange(item.categoryKey, { suggestedMessageTemplate: e.target.value })
                                  }
                                  className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-2 py-1 text-xs min-h-16"
                                />
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <div className="rounded-xl bg-white/5 border border-white/10 p-4 space-y-4">
                    <div className="flex items-center justify-between gap-3 flex-wrap">
                      <h3 className="text-sm uppercase tracking-wider text-white/70">Bloqueios</h3>
                      <button
                        onClick={handleSaveCrmBlock}
                        disabled={isLoadingCrm || !adminToken.trim()}
                        className="px-4 py-2 rounded-lg bg-brand-green text-black text-xs uppercase tracking-wider disabled:opacity-50"
                      >
                        Bloquear cliente
                      </button>
                    </div>
                    <div className="grid gap-2">
                      <input
                        value={crmBlockClientName}
                        onChange={(e) => setCrmBlockClientName(e.target.value)}
                        placeholder="Nome da cliente"
                        className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                      />
                      <input
                        value={crmBlockPhone}
                        onChange={(e) => setCrmBlockPhone(normalizeDigits(e.target.value))}
                        placeholder="Telefone"
                        className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                      />
                      <select
                        value={crmBlockReason}
                        onChange={(e) => setCrmBlockReason(e.target.value)}
                        className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                      >
                        <option value="manual_block">Bloqueio manual</option>
                        <option value="opt_out">Nao quer receber</option>
                        <option value="sensitive_case">Caso sensivel</option>
                        <option value="spam_risk">Risco de spam</option>
                      </select>
                      <textarea
                        value={crmBlockNotes}
                        onChange={(e) => setCrmBlockNotes(e.target.value)}
                        placeholder="Observacao interna"
                        className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm min-h-20"
                      />
                    </div>
                    <div className="space-y-2 max-h-[320px] overflow-y-auto pr-1">
                      {!crmBlocks.length && (
                        <p className="text-sm text-white/60">Nenhuma cliente bloqueada ainda.</p>
                      )}
                      {crmBlocks.map((item) => (
                        <div key={`crm-block-${item.id || item.phone}`} className="rounded-lg bg-white/5 border border-white/10 p-3">
                          <div className="text-sm text-white">{item.clientName || item.phone}</div>
                          <div className="text-xs text-white/60">
                            {item.phone} | {item.blockReason || 'sem motivo'}
                          </div>
                          {item.blockNotes && <div className="text-xs text-white/75 mt-1">{item.blockNotes}</div>}
                        </div>
                      ))}
                    </div>
                  </div>
                </div>

                <div className="grid xl:grid-cols-2 gap-4">
                  <div className="rounded-xl bg-white/5 border border-white/10 p-4">
                    <p className="text-sm uppercase tracking-wider text-white/70 mb-3">Fluxos Atuais</p>
                    <div className="space-y-2 max-h-[320px] overflow-y-auto pr-1">
                      {!crmFlows.length && <p className="text-sm text-white/60">Ainda nao existem fluxos operacionais criados.</p>}
                      {crmFlows.map((item) => {
                        const flowId = item.id ?? 0;
                        const isExpanded = expandedFlowId === flowId;
                        const isActionable = ['pending_approval', 'eligible', 'scheduled_step_1'].includes(item.flowStatus || '');
                        const isStoppable = !['converted', 'stopped', 'expired', 'opted_out'].includes(item.flowStatus || '');
                        const isLoadingThis = crmFlowActionLoading === flowId;
                        return (
                          <div key={`crm-flow-${flowId || item.phone}`} className="rounded-lg bg-white/5 border border-white/10 p-3 space-y-2">
                            <div className="flex items-start justify-between gap-2">
                              <div className="min-w-0 flex-1">
                                <div className="text-sm text-white">{item.clientName || item.phone || 'Cliente sem nome'}</div>
                                <div className="text-xs text-white/60">
                                  {item.originServiceName || 'sem servico'} | etapa {item.currentStep || 0} | {item.flowStatus || 'sem status'}
                                </div>
                                <div className="text-xs text-white/75">
                                  Ultimo profissional: {item.lastProfessionalName || 'nao informado'}
                                  {item.lastProfessionalActive === false ? ' (inativo)' : ''}
                                </div>
                              </div>
                              <div className="flex gap-1 flex-shrink-0">
                                {isActionable && flowId > 0 && (
                                  <button
                                    onClick={() => handleApproveCrmFlow(flowId)}
                                    disabled={isLoadingThis}
                                    className="px-2 py-1 rounded bg-green-600/80 text-white text-xs disabled:opacity-50"
                                    title="Enviar etapa 1 e aprovar fluxo"
                                  >
                                    {isLoadingThis ? '...' : 'Aprovar'}
                                  </button>
                                )}
                                {isStoppable && flowId > 0 && (
                                  <button
                                    onClick={() => handleStopCrmFlow(flowId)}
                                    disabled={isLoadingThis}
                                    className="px-2 py-1 rounded bg-red-600/70 text-white text-xs disabled:opacity-50"
                                    title="Encerrar este fluxo"
                                  >
                                    {isLoadingThis ? '...' : 'Parar'}
                                  </button>
                                )}
                                {flowId > 0 && (
                                  <button
                                    onClick={() => handleToggleCrmFlowEvents(flowId)}
                                    className="px-2 py-1 rounded bg-white/10 text-white/70 text-xs"
                                    title="Ver historico de eventos"
                                  >
                                    {isExpanded ? '▲' : '▼'}
                                  </button>
                                )}
                              </div>
                            </div>
                            {isExpanded && (
                              <div className="border-t border-white/10 pt-2 space-y-1">
                                {(crmFlowEvents[flowId] || []).length === 0 && (
                                  <p className="text-xs text-white/50">Sem eventos registrados.</p>
                                )}
                                {(crmFlowEvents[flowId] || []).map((evt, idx) => (
                                  <div key={idx} className="text-xs text-white/70 flex gap-2">
                                    <span className="text-white/40 flex-shrink-0">{evt.createdAt?.slice(0, 16).replace('T', ' ')}</span>
                                    <span className="uppercase tracking-wide text-white/60">{evt.eventType}</span>
                                    {evt.step != null && <span>etapa {evt.step}</span>}
                                    {evt.replySummary && <span className="truncate italic">{evt.replySummary}</span>}
                                    {evt.messageSent && !evt.replySummary && <span className="truncate">{evt.messageSent.slice(0, 80)}</span>}
                                  </div>
                                ))}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </div>

                  <div className="rounded-xl bg-white/5 border border-white/10 p-4">
                    <p className="text-sm uppercase tracking-wider text-white/70 mb-3">Oportunidades</p>
                    <div className="space-y-2 max-h-[320px] overflow-y-auto pr-1">
                      {!crmOpportunities.length && <p className="text-sm text-white/60">Nenhuma oportunidade estrategica registrada ainda.</p>}
                      {crmOpportunities.map((item) => (
                        <div key={`crm-opportunity-${item.id || `${item.phone}-${item.categoryName}`}`} className="rounded-lg bg-white/5 border border-white/10 p-3">
                          <div className="text-sm text-white">{item.clientName || item.phone || 'Cliente sem nome'}</div>
                          <div className="text-xs text-white/60">
                            {item.categoryName || 'sem categoria'} | {item.opportunityStatus || 'open'} | prioridade {item.priority || 'medium'}
                          </div>
                          <div className="text-xs text-white/75 mt-1">
                            Ultimo profissional: {item.lastProfessionalName || 'nao informado'}
                            {item.lastProfessionalActive === false ? ' (inativo)' : ''}
                          </div>
                        </div>
                      ))}
                    </div>
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
                  {isSuperAdminSession ? (
                    <>
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
                    </>
                  ) : (
                    <>
                      <p className="text-xs uppercase tracking-wider text-white/60">Conta logada</p>
                      <div className="flex flex-wrap items-center gap-2 text-xs text-white/80">
                        <span>
                          Sessao atual: <strong>{adminPrincipal?.role || 'tenant'}</strong>
                        </span>
                        {sessionTenantName && (
                          <span className="inline-flex items-center rounded-full px-3 py-1 bg-brand-blue/20 border border-brand-blue/40 text-brand-blue font-semibold">
                            Tenant: {sessionTenantName}
                            {sessionTenantCode ? ` (${sessionTenantCode})` : ''}
                          </span>
                        )}
                        {adminPrincipal?.username && <span>| usuario: {adminPrincipal.username}</span>}
                      </div>
                    </>
                  )}
                  {adminStatus && <p className="text-xs text-brand-green">{adminStatus}</p>}
                </div>

                {isSuperAdminSession ? (
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
                              <option value="evolution">evolution</option>
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
                ) : (
                  <div className="rounded-xl bg-white/5 border border-white/10 p-4">
                    <p className="text-xs uppercase tracking-wider text-white/60">Visao do cliente</p>
                    <p className="text-sm text-white/80 mt-2">
                      Esta sessao tem acesso apenas ao proprio salao
                      {sessionTenantName ? `: ${sessionTenantName}` : ''}.
                    </p>
                    {sessionTenantCode && (
                      <p className="text-xs text-white/60 mt-1">Tenant code: {sessionTenantCode}</p>
                    )}
                  </div>
                )}
              </>
            )}

            {activeSection === 'salon' && (
              <>
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <h2 className="heading-bold text-lg text-white">Estabelecimento</h2>
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
                  {renderToneSelector()}
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
                  <h2 className="heading-bold text-lg text-white">FAQ</h2>
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

                <div className="rounded-lg bg-white/5 border border-white/10 p-4">
                  <p className="text-sm text-white/80">
                    As acoes de Marketing (MKT) agora ficam na aba <span className="text-white font-semibold">Marketing</span>.
                  </p>
                  <button
                    onClick={() => setActiveSection('knowledge')}
                    className="mt-3 px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider"
                  >
                    Ir para Marketing
                  </button>
                </div>
              </>
            )}

            {activeSection === 'whatsapp' && (
              <>
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <h2 className="heading-bold text-lg text-white">Conexao WhatsApp</h2>
                </div>
                <p className="text-xs text-white/60">
                  Conecte e monitore o WhatsApp desta unidade em uma categoria separada.
                </p>
                {renderWhatsappConnectionBlock()}
              </>
            )}

            {activeSection === 'knowledge' && (
              <>
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <h2 className="heading-bold text-lg text-white">Marketing</h2>
                  <button
                    onClick={handleSaveKnowledge}
                    disabled={isSavingKnowledge || isLoadingKnowledge}
                    className="px-4 py-2 rounded-lg bg-brand-blue text-white text-xs uppercase tracking-wider disabled:opacity-50"
                  >
                    {isSavingKnowledge ? 'Salvando...' : 'Salvar'}
                  </button>
                </div>
                <p className="text-xs text-white/60">
                  Configure abaixo apenas as acoes de marketing e clique em Salvar.
                </p>
                {knowledgeStatus && <p className={`text-xs ${knowledgeStatusClass}`}>{knowledgeStatus}</p>}

                <div className="flex items-center justify-between gap-3 flex-wrap pt-4">
                  <h3 className="heading-bold text-base text-white">Acoes de Marketing</h3>
                  <button
                    onClick={() =>
                      updateKnowledge((draft) => {
                        const currentMarketing =
                          draft.marketing && typeof draft.marketing === 'object' ? draft.marketing : {};
                        const currentActions = Array.isArray(currentMarketing.actions) ? currentMarketing.actions : [];
                        const nextIndex = currentActions.length + 1;
                        draft.marketing = {
                          ...currentMarketing,
                          actions: [
                            ...currentActions,
                            {
                              id: `acao-${nextIndex}`,
                              name: `Acao ${nextIndex}`,
                              type: 'upsell',
                              trigger: 'before_closing',
                              enabled: true,
                              message: '',
                              endDate: '',
                              mediaUrl: '',
                              mediaCaption: '',
                            },
                          ],
                        };
                      })
                    }
                    className="px-3 py-2 rounded-lg bg-white/10 text-white text-xs uppercase tracking-wider"
                  >
                    + Acao MKT
                  </button>
                </div>

                <label className="flex items-center gap-2 text-sm text-white/80">
                  <input
                    type="checkbox"
                    checked={marketingEnabled}
                    onChange={(e) =>
                      updateKnowledge((draft) => {
                        const currentMarketing =
                          draft.marketing && typeof draft.marketing === 'object' ? draft.marketing : {};
                        draft.marketing = {
                          ...currentMarketing,
                          enabled: e.target.checked,
                          actions: Array.isArray(currentMarketing.actions) ? currentMarketing.actions : [],
                        };
                      })
                    }
                  />
                  Habilitar ambiente de marketing
                </label>

                {!marketingActions.length && (
                  <p className="text-white/70 text-sm">
                    Nenhuma acao cadastrada. Adicione uma acao de upsell/cross-sell para enviar no fechamento.
                  </p>
                )}
                {marketingActions.length > 0 && (
                  <div className="space-y-3">
                    {marketingActions.map((action: any, idx: number) => (
                      <div key={`marketing-action-${idx}`} className="rounded-lg bg-white/5 border border-white/10 p-3 space-y-2">
                        <div className="grid md:grid-cols-2 gap-2">
                          <input
                            value={toText(action?.name)}
                            onChange={(e) =>
                              updateKnowledge((draft) => {
                                const currentMarketing =
                                  draft.marketing && typeof draft.marketing === 'object' ? draft.marketing : {};
                                const currentActions = Array.isArray(currentMarketing.actions) ? currentMarketing.actions : [];
                                currentActions[idx] = { ...(currentActions[idx] || {}), name: e.target.value };
                                draft.marketing = { ...currentMarketing, actions: currentActions };
                              })
                            }
                            placeholder="Nome da acao"
                            className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                          />
                          <input
                            value={toText(action?.id)}
                            onChange={(e) =>
                              updateKnowledge((draft) => {
                                const currentMarketing =
                                  draft.marketing && typeof draft.marketing === 'object' ? draft.marketing : {};
                                const currentActions = Array.isArray(currentMarketing.actions) ? currentMarketing.actions : [];
                                currentActions[idx] = { ...(currentActions[idx] || {}), id: e.target.value };
                                draft.marketing = { ...currentMarketing, actions: currentActions };
                              })
                            }
                            placeholder="ID da acao"
                            className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                          />
                        </div>

                        <div className="grid md:grid-cols-2 gap-2">
                          <select
                            value={toText(action?.type) || 'upsell'}
                            onChange={(e) =>
                              updateKnowledge((draft) => {
                                const currentMarketing =
                                  draft.marketing && typeof draft.marketing === 'object' ? draft.marketing : {};
                                const currentActions = Array.isArray(currentMarketing.actions) ? currentMarketing.actions : [];
                                currentActions[idx] = { ...(currentActions[idx] || {}), type: e.target.value };
                                draft.marketing = { ...currentMarketing, actions: currentActions };
                              })
                            }
                            className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                          >
                            <option value="upsell">upsell</option>
                            <option value="cross_sell">cross_sell</option>
                            <option value="custom">custom</option>
                          </select>
                          <select
                            value={toText(action?.trigger) || 'before_closing'}
                            onChange={(e) =>
                              updateKnowledge((draft) => {
                                const currentMarketing =
                                  draft.marketing && typeof draft.marketing === 'object' ? draft.marketing : {};
                                const currentActions = Array.isArray(currentMarketing.actions) ? currentMarketing.actions : [];
                                currentActions[idx] = { ...(currentActions[idx] || {}), trigger: e.target.value };
                                draft.marketing = { ...currentMarketing, actions: currentActions };
                              })
                            }
                            className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                          >
                            <option value="before_closing">before_closing (encerrar atendimento)</option>
                            <option value="always">always (todas respostas)</option>
                          </select>
                        </div>

                        <div className="grid md:grid-cols-2 gap-2">
                          <input
                            type="date"
                            value={toText(action?.endDate)}
                            onChange={(e) =>
                              updateKnowledge((draft) => {
                                const currentMarketing =
                                  draft.marketing && typeof draft.marketing === 'object' ? draft.marketing : {};
                                const currentActions = Array.isArray(currentMarketing.actions) ? currentMarketing.actions : [];
                                currentActions[idx] = { ...(currentActions[idx] || {}), endDate: e.target.value };
                                draft.marketing = { ...currentMarketing, actions: currentActions };
                              })
                            }
                            className="rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                          />
                          <div className="text-xs text-white/65 flex items-center px-2">
                            Data final da acao (inclusive). Se vencer, nao envia.
                          </div>
                        </div>

                        <label className="flex items-center gap-2 text-sm text-white/80">
                          <input
                            type="checkbox"
                            checked={action?.enabled !== false}
                            onChange={(e) =>
                              updateKnowledge((draft) => {
                                const currentMarketing =
                                  draft.marketing && typeof draft.marketing === 'object' ? draft.marketing : {};
                                const currentActions = Array.isArray(currentMarketing.actions) ? currentMarketing.actions : [];
                                currentActions[idx] = { ...(currentActions[idx] || {}), enabled: e.target.checked };
                                draft.marketing = { ...currentMarketing, actions: currentActions };
                              })
                            }
                          />
                          Acao habilitada
                        </label>

                        <textarea
                          value={toText(action?.message)}
                          onChange={(e) =>
                            updateKnowledge((draft) => {
                              const currentMarketing =
                                draft.marketing && typeof draft.marketing === 'object' ? draft.marketing : {};
                              const currentActions = Array.isArray(currentMarketing.actions) ? currentMarketing.actions : [];
                              currentActions[idx] = { ...(currentActions[idx] || {}), message: e.target.value };
                              draft.marketing = { ...currentMarketing, actions: currentActions };
                            })
                          }
                          placeholder="Mensagem da acao (ex: Temos pacote de hidratacao com 15% hoje. Quer aproveitar?)"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm min-h-20"
                        />

                        <input
                          value={toText(action?.mediaUrl)}
                          onChange={(e) =>
                            updateKnowledge((draft) => {
                              const currentMarketing =
                                draft.marketing && typeof draft.marketing === 'object' ? draft.marketing : {};
                              const currentActions = Array.isArray(currentMarketing.actions) ? currentMarketing.actions : [];
                              currentActions[idx] = { ...(currentActions[idx] || {}), mediaUrl: e.target.value };
                              draft.marketing = { ...currentMarketing, actions: currentActions };
                            })
                          }
                          placeholder="URL da imagem (opcional) - ex: https://.../oferta.jpg"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                        />

                        <input
                          value={toText(action?.mediaCaption)}
                          onChange={(e) =>
                            updateKnowledge((draft) => {
                              const currentMarketing =
                                draft.marketing && typeof draft.marketing === 'object' ? draft.marketing : {};
                              const currentActions = Array.isArray(currentMarketing.actions) ? currentMarketing.actions : [];
                              currentActions[idx] = { ...(currentActions[idx] || {}), mediaCaption: e.target.value };
                              draft.marketing = { ...currentMarketing, actions: currentActions };
                            })
                          }
                          placeholder="Legenda da imagem (opcional, usa a mensagem da acao se vazio)"
                          className="w-full rounded-md bg-[#0f1731] border border-white/15 text-white/90 px-3 py-2 text-sm"
                        />

                        <div className="flex items-center gap-2 flex-wrap">
                          <label
                            className={`px-3 py-2 rounded-md text-xs uppercase tracking-wider border ${
                              uploadingMarketingIndex === idx
                                ? 'bg-white/5 border-white/10 text-white/50 cursor-not-allowed'
                                : 'bg-white/10 border-white/20 text-white cursor-pointer hover:bg-white/15'
                            }`}
                          >
                            <input
                              type="file"
                              accept="image/png,image/jpeg,image/jpg,image/webp,image/gif"
                              className="hidden"
                              disabled={uploadingMarketingIndex !== null}
                              onChange={(e) => {
                                const file = e.target.files?.[0] || null;
                                e.currentTarget.value = '';
                                void handleUploadMarketingImage(idx, file);
                              }}
                            />
                            {uploadingMarketingIndex === idx ? 'Enviando imagem...' : 'Upload imagem'}
                          </label>

                          {toText(action?.mediaUrl) && (
                            <a
                              href={toText(action?.mediaUrl)}
                              target="_blank"
                              rel="noreferrer"
                              className="text-xs text-brand-blue hover:text-white"
                            >
                              Abrir imagem atual
                            </a>
                          )}
                        </div>
                        <p className="text-[11px] text-white/60">
                          Upload direto pela plataforma (png, jpg, webp ou gif).
                        </p>

                        <button
                          onClick={() =>
                            updateKnowledge((draft) => {
                              const currentMarketing =
                                draft.marketing && typeof draft.marketing === 'object' ? draft.marketing : {};
                              const currentActions = Array.isArray(currentMarketing.actions) ? currentMarketing.actions : [];
                              draft.marketing = {
                                ...currentMarketing,
                                actions: currentActions.filter((_: any, currentIdx: number) => currentIdx !== idx),
                              };
                            })
                          }
                          className="text-xs text-red-300 hover:text-red-200"
                        >
                          Remover acao
                        </button>
                      </div>
                    ))}
                  </div>
                )}
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
          {isSuperAdminSession && (
            <button
              onClick={() => setActiveSection('admin')}
              className="px-4 py-2 rounded-full glass label-micro text-white/60 hover:text-white hover:border-white/30 transition-all flex items-center gap-2"
            >
              <Sparkles className="w-3 h-3 text-white/70" /> Admin
            </button>
          )}
          <button
            onClick={() => setActiveSection('crm')}
            className="px-4 py-2 rounded-full glass label-micro text-white/60 hover:text-white hover:border-white/30 transition-all flex items-center gap-2"
          >
            <Sparkles className="w-3 h-3 text-white/70" /> CRM
          </button>
          <button
            onClick={() => setActiveSection('whatsapp')}
            className="px-4 py-2 rounded-full glass label-micro text-white/60 hover:text-white hover:border-white/30 transition-all flex items-center gap-2"
          >
            <Phone className="w-3 h-3 text-white/70" /> Conexao WhatsApp
          </button>
          <button
            onClick={() => setActiveSection('knowledge')}
            className="px-4 py-2 rounded-full glass label-micro text-white/60 hover:text-white hover:border-white/30 transition-all flex items-center gap-2"
          >
            <Sparkles className="w-3 h-3 text-white/70" /> Marketing
          </button>
          <button
            onClick={() => setActiveSection('history')}
            className="px-4 py-2 rounded-full glass label-micro text-white/60 hover:text-white hover:border-white/30 transition-all flex items-center gap-2"
          >
            <Calendar className="w-3 h-3 text-white/70" /> Historico
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
