# Kafka 실전 학습 프로젝트 — 이커머스 주문 처리 (Choreography Saga)

Kafka를 실무에서 가장 흔히 쓰이는 형태(이벤트 기반 마이크로서비스 간 통신)로 익히기 위한 예제 프로젝트.

## 시나리오

이커머스 주문 처리: 주문 생성 → 재고 확인/차감 → 결제 → 알림. 각 단계가 별도의 Spring Boot
마이크로서비스로 분리되어 있고, **오직 Kafka 이벤트로만 통신**한다 (서비스 간 직접 REST 호출 없음).

**Choreography 기반 Saga 패턴**을 사용한다 — 별도의 오케스트레이터 없이 각 서비스가 이벤트를
구독해 자율적으로 다음 액션을 수행한다. 결제 실패 시 `inventory-service`가 `payment.failed`를
구독해 재고를 복원하는 **보상 트랜잭션(compensating transaction)**까지 포함한다.

## 버전

- Java 17, Spring Boot 4.1.x (Spring Framework 7) — Spring Boot 3.x는 2026-06-30부로 OSS EOL
- Kafka: `apache/kafka:4.3.1` 공식 이미지, KRaft 모드 (Zookeeper 불필요)
- Kafka UI: `provectuslabs/kafka-ui` (토픽/메시지/컨슈머 그룹 확인용)

## 아키텍처

```
                         ┌──────────────────┐
   POST /orders   ─────► │  order-service    │
                         │  (REST + Producer │
                         │   + Saga listener)│
                         └─────────┬─────────┘
                                   │ order.created (key=orderId)
                                   ▼
                         ┌──────────────────┐
                         │ inventory-service │
                         │ (재고 차감/복원)   │
                         └─────────┬─────────┘
                     inventory.reserved / inventory.failed
                                   ▼
                         ┌──────────────────┐
                         │  payment-service  │
                         │  (결제 시뮬레이션) │
                         └─────────┬─────────┘
                     payment.completed / payment.failed
                                   │
                 ┌─────────────────┼─────────────────────┐
                 ▼                 ▼                     ▼
        order-service       notification-service   inventory-service
      (주문 상태 갱신)      (알림 로그 발송)      (결제 실패 시 재고 복원)
```

## Kafka 토픽

| 토픽 | Producer | Consumer(s) | 설명 |
|---|---|---|---|
| `order.created` | order-service | inventory-service | 주문 생성 |
| `inventory.reserved` | inventory-service | payment-service | 재고 확보 성공 |
| `inventory.failed` | inventory-service | order-service, notification-service | 재고 부족 |
| `payment.completed` | payment-service | order-service, notification-service | 결제 성공 |
| `payment.failed` | payment-service | order-service, notification-service, inventory-service | 결제 실패 (재고 복원 트리거) |

모든 이벤트는 **orderId를 메시지 key**로 사용 → 동일 주문의 이벤트는 항상 같은 파티션에 순서대로 적재.

## 모듈 구조

```
kafka/
├── settings.gradle
├── build.gradle                  # 공통 플러그인/의존성 버전 관리
├── docker-compose.yml            # Kafka(KRaft) + Kafka UI
├── common-events/                 # 공유 이벤트 DTO (Java record)
├── order-service/                 # REST API, Producer, Saga 상태갱신 Consumer (port 8081)
├── inventory-service/             # 재고 Consumer/Producer, 보상 트랜잭션 (port 8082)
├── payment-service/                # 결제 시뮬레이션 Consumer/Producer (port 8083)
└── notification-service/           # 알림 Consumer, 로그 시뮬레이션 (port 8084)
```

각 서비스는 독립 실행 가능한 Spring Boot 애플리케이션이며 상태 저장은 H2(파일 모드)로 단순화했다.

## 핵심 실무 패턴

1. **Producer 신뢰성**: `enable.idempotence=true`, `acks=all` 명시적 설정
2. **재시도 + DLQ**: `DefaultErrorHandler` + `ExponentialBackOff`(3회) → 최종 실패 시
   `DeadLetterPublishingRecoverer`로 `<topic>.DLT`로 전송
3. **멱등 컨슈머**: 각 서비스가 `processed_events(event_id PK)` 테이블(H2)에 이벤트 ID를 기록,
   중복 수신 시 스킵 (Kafka at-least-once 특성 대응)
4. **컨슈머 그룹**: 서비스별 고유 `group-id`
5. **Kafka UI**: 토픽/메시지/컨슈머 그룹을 브라우저(`localhost:8090`)에서 직접 확인

## 로컬 실행

```bash
# 1. Kafka + Kafka UI 기동
docker compose up -d

# 2. 각 서비스 개별 기동 (별도 터미널)
./gradlew :order-service:bootRun
./gradlew :inventory-service:bootRun
./gradlew :payment-service:bootRun
./gradlew :notification-service:bootRun

# 3. 주문 생성
curl -X POST localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"productId": "P001", "quantity": 2}'

# 4. 주문 상태 확인
curl localhost:8081/orders/{orderId}
```

Kafka UI: http://localhost:8090

## 검증 시나리오

- **정상 흐름**: 재고가 충분한 상품 주문 → `CONFIRMED`로 전이
- **재고 부족**: 재고보다 많은 수량 주문 → `inventory.failed` → `CANCELLED`
- **결제 실패**: 결제 실패를 유발하는 조건(예: 특정 금액 이상) → `payment.failed` →
  재고 복원(보상 트랜잭션) → `CANCELLED`
- **재시도/DLQ**: 컨슈머 예외 강제 발생 → 재시도 로그 확인 → 최종 실패 시 `<topic>.DLT`로 이관되는지
  Kafka UI에서 확인
