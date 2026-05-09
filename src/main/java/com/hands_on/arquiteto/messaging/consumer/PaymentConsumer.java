package com.hands_on.arquiteto.messaging.consumer;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import com.hands_on.arquiteto.config.RabbitConfig;
import com.hands_on.arquiteto.messaging.payload.OrderCreatedEvent;
import com.hands_on.arquiteto.messaging.payload.PaymentProcessedEvent;


/**
 * ============ 📥 PAYMENT CONSUMER — PROCESSADOR DE EVENTOS DE PAGAMENTO ============
 *
 * 🧠 VISÃO GERAL: Este componente é responsável por consumir eventos de negócio relacionados à
 * criação de pedidos e executar o processamento de pagamento de forma assíncrona.
 *
 * Ele faz parte de uma arquitetura orientada a eventos (EDA), onde serviços reagem a eventos ao
 * invés de se comunicarem diretamente.
 *
 * --------------- 📡 FLUXO DO SISTEMA ---------------
 *
 * OrderService ↓ OrderEventPublisher ↓ OrderCreatedEvent ↓ RabbitMQ (payment.queue) ↓
 * PaymentConsumer (este componente) ↓ PaymentProcessedEvent ↓ EmailConsumer
 *
 * ----------------- 🎯 RESPONSABILIDADES -----------------
 *
 * ✅ Consumir evento OrderCreatedEvent ✅ Processar pagamento do pedido ✅ Garantir idempotência
 * (evitar duplicidade) ✅ Publicar próximo evento do fluxo (PaymentProcessedEvent)
 *
 * ---------------- 🔒 IDEMPOTÊNCIA (REGRA CRÍTICA) ----------------
 *
 * Este consumer pode receber a mesma mensagem mais de uma vez (retry ou reentrega).
 *
 * Portanto, deve garantir:
 *
 * ✅ Um pedido NÃO pode ser pago duas vezes
 *
 * Estratégia recomendada (produção): - Consultar status do pedido no banco - Se já estiver pago →
 * ignorar
 *
 * Problemas evitados: ❌ Cobrança duplicada ❌ Inconsistência de dados
 *
 * ---------------- 🛡️ RESILIÊNCIA ----------------
 *
 * - Retry automático configurado via Spring - Backoff entre tentativas - Integração com Dead Letter
 * Queue (DLQ)
 *
 * Fluxo de erro:
 *
 * payment.queue → retry → DLX → payment.dlq
 *
 * ------------------ ⚙️ BOAS PRÁTICAS (PRODUÇÃO) -----------------
 *
 * - Substituir System.out por logs estruturados (ex: Logback) - Utilizar correlationId (MDC) para
 * rastreamento - Tratar exceções de forma controlada - Aplicar controle de concorrência (optimistic
 * locking)
 *
 */

@Service // Registra esta classe como um Bean gerenciado pelo Spring
public class PaymentConsumer {


    /**
     * 📤 RabbitTemplate
     *
     * Responsável por publicar novos eventos após o processamento.
     *
     * Neste fluxo: PaymentProcessedEvent → próximo passo do domínio
     */
    private final RabbitTemplate rabbitTemplate;

    public PaymentConsumer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }



    /**
     * ========== 📥 CONSUMIDOR PRINCIPAL — ORDER CREATED EVENT ==========
     *
     * 🧠 Este método reage ao evento:
     *
     * 👉 "Um pedido foi criado"
     *
     * ---------------- 🔁 FLUXO EXECUTADO ------------
     *
     * 1. Recebe OrderCreatedEvent 2. Processa pagamento 3. Publica PaymentProcessedEvent
     *
     * -------------- 🎯 OBJETIVO --------------
     *
     * Converter um evento de criação de pedido em um evento de pagamento processado, mantendo o
     * sistema desacoplado e baseado em eventos de domínio.
     *
     * ------------- 🚀 CONCURRENCY (ESCALABILIDADE CRÍTICA) ------------
     *
     * Este consumer utiliza:
     *
     * 👉 concurrency = "5-20"
     *
     * O que isso significa:
     *
     * ✅ 5 → número mínimo de consumidores (threads) sempre ativos ✅ 20 → número máximo de
     * consumidores sob alta demanda
     *
     * ----------- 🔄 COMPORTAMENTO EM TEMPO DE EXECUÇÃO ----------
     *
     * - A aplicação inicia com 5 consumidores processando mensagens em paralelo - Se houver
     * crescimento da fila (READY ↑), o Spring aumenta automaticamente o número de consumidores -
     * Pode escalar dinamicamente até 20 consumidores - Quando a carga diminui, volta gradualmente
     * para 10
     *
     * 👉 Isso implementa auto scaling interno baseado em pressão da fila
     *
     * ---------- 📈 IMPACTO NA PERFORMANCE ---------
     *
     * Mais consumidores: ✅ aumentam significativamente o throughput (mensagens/segundo) ✅ reduzem
     * tempo total de processamento da fila ✅ diminuem backlog (READY ↓)
     *
     * Porém: ⚠️ aumentam uso de CPU ⚠️ aumentam concorrência no banco (UPDATE / SELECT) ⚠️ aumentam
     * pressão em serviços externos (gateway de pagamento)
     *
     * ----------- ⚠️ ATENÇÃO: ESTE É UM FLUXO CRÍTICO (PAGAMENTO) ----------
     *
     * Diferente do envio de e-mail, este consumer executa uma operação sensível:
     *
     * 👉 cobrança / processamento financeiro
     *
     * Portanto:
     *
     * ❗ ESTE CONSUMER PRECISA SER IDEMPOTENTE
     *
     * Mesmo evento NÃO pode gerar: ❌ cobrança duplicada ❌ inconsistência financeira
     *
     * 👉 Boa prática:
     *
     * - Verificar status do pedido antes de processar - Se já estiver "PROCESSED" → NÃO executar
     * novamente
     *
     * ------------ ✅ QUANDO USAR ALTA CONCURRENCY -----------
     *
     * ✔ Sistemas com alto volume de pedidos (alta taxa de criação) ✔ Processamento intensivo que
     * precisa escalar horizontalmente ✔ Cenários com backlog frequente (READY alto)
     *
     * ----------- ❌ QUANDO EVITAR ALTA CONCURRENCY -----------
     *
     * ✖ Integrações com sistemas lentos (ex: gateway que limita requisições) ✖ Banco com alto nível
     * de contenção (lock/transactions) ✖ Falta de idempotência (risco de duplicidade)
     *
     * ----------- ⚠️ RISCOS SE MAL CONFIGURADO ----------
     *
     * - Concorrência elevada sem controle → condição de corrida (race condition) - Atualizações
     * simultâneas no mesmo pedido - Deadlocks no banco - Sobrecarga de APIs externas
     *
     * ----------- ⚙️ BOAS PRÁTICAS -------------
     *
     * ✔ Garantir idempotência (ESSENCIAL) ✔ Utilizar controle de concorrência (optimistic locking /
     * versionamento) ✔ Monitorar métricas: - Queue READY - Unacked - Ack rate ✔ Ajustar valores
     * conforme carga real (tuning contínuo)
     *
     * ------------ 💡 ANALOGIA ------------
     *
     * - 5 consumidores → equipe fixa de processamento - até 20 consumidores → reforço em momentos
     * de alta demanda
     *
     * 👉 Aqui você está escalando um serviço de pagamento em tempo real
     *
     */

    @RabbitListener(queues = RabbitConfig.PAYMENT_QUEUE, concurrency = "5-20")
    public void process(OrderCreatedEvent event) {

        // ✅ 1. Processa pagamento (simulado)
        processPayment(event);

        // ✅ 2. Cria o próximo evento de negócio: PaymentProcessedEvent
        PaymentProcessedEvent paymentProcessedEvent = new PaymentProcessedEvent(event.orderId());

        // ✅ 3. Publica o evento para continuidade do fluxo
        rabbitTemplate.convertAndSend(RabbitConfig.PAYMENT_EXCHANGE, RabbitConfig.PAYMENT_PROCESSED,
                paymentProcessedEvent);


        // 👉 ideal: usar logger
        System.out.println("✅ PaymentProcessedEvent publicado | orderId=" + event.orderId());

    }


    /**
     * --------------------------------------------------------------------------------- 💳
     * PROCESSAMENTO DE PAGAMENTO
     * ---------------------------------------------------------------------------------
     *
     * 🧠 Contém a lógica de negócio responsável por processar o pagamento.
     *
     * Em ambiente real, envolveria:
     *
     * - Integração com gateway de pagamento - Validação antifraude - Atualização do status no banco
     *
     * --------------------------------------------------------------------------------- ⚠️ REGRA
     * CRÍTICA ---------------------------------------------------------------------------------
     *
     * Este método deve ser IDEMPOTENTE:
     *
     * Mesmo evento NÃO pode gerar cobrança duplicada.
     *
     */

    private void processPayment(OrderCreatedEvent event) {
        // Simulação de processamento de pagamento

        try {
            System.out.println("Processando pagamento: " + event.orderId());
            Thread.sleep(200); // simula processamento lento
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("Pagamento concluído");
    }



    /**
     * ================================================================================= ☠️
     * CONSUMIDOR DE ERROS (DEAD LETTER QUEUE)
     * =================================================================================
     *
     * 🧠 Este método trata eventos que falharam definitivamente no processamento.
     *
     * --------------------------------------------------------------------------------- 📡 ORIGEM
     * ---------------------------------------------------------------------------------
     *
     * payment.queue → falha → retry → DLX → payment.dlq
     *
     * --------------------------------------------------------------------------------- 🎯 OBJETIVO
     * ---------------------------------------------------------------------------------
     *
     * - Registrar falha - Permitir auditoria - Evitar perda de mensagem
     *
     * --------------------------------------------------------------------------------- ⚠️ REGRAS
     * IMPORTANTES ---------------------------------------------------------------------------------
     *
     * ❌ NÃO reprocessar automaticamente ❌ NÃO gerar retry infinito
     *
     * ✅ Tratar como erro definitivo
     *
     * --------------------------------------------------------------------------------- 🚀
     * MELHORIAS FUTURAS
     * ---------------------------------------------------------------------------------
     *
     * - Persistir erro em banco - Enviar alertas (Slack, email) - Criar fluxo de reprocessamento
     * manual - Adicionar correlationId
     *
     */
    @RabbitListener(queues = RabbitConfig.PAYMENT_DLQ)
    public void handleError(OrderCreatedEvent event) {

        // ==========================================================
        // 📥 RECEBIMENTO DE MENSAGEM COM ERRO
        // ==========================================================
        //
        // Mensagem já foi considerada falha após retries
        //
        System.out.println("☠️ Pedido enviado para DLQ: " + event.orderId());

        // ==========================================================
        // 📌 POSSÍVEIS AÇÕES (FUTURO)
        // ==========================================================
        //
        // Aqui você poderia:
        //
        // - Salvar erro em banco
        // - Notificar times (Slack, email, etc.)
        // - Disparar reprocessamento manual
        // - Gerar métricas (monitoramento)
        //
        // Exemplo:
        // errorRepository.save(...)

    }
}

