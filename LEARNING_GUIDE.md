# Kafka + Spring Boot 학습 가이드 (초보자용)

이 문서는 이 저장소(`kafka-order-saga`)를 통해 **Kafka와 Java/Spring Boot를 실전 수준으로 학습**할 수
있도록, 개념 → 설계 → 코드 → 실행 → 검증 → 트러블슈팅까지 전부 담았습니다. 길지만 순서대로 읽으면
"왜 이렇게 만들었는지"까지 이해할 수 있게 구성했습니다.

빠르게 실행만 하고 싶다면 [10. 실행 방법 총정리](#10-실행-방법-총정리-치트시트)로 바로 이동하세요.

---

## 목차

1. [이 프로젝트가 푸는 문제](#1-이-프로젝트가-푸는-문제)
2. [Kafka 핵심 개념 먼저 이해하기](#2-kafka-핵심-개념-먼저-이해하기)
3. [아키텍처: Choreography Saga](#3-아키텍처-choreography-saga)
4. [로컬 인프라: docker-compose.yml 완전 분석](#4-로컬-인프라-docker-composeyml-완전-분석)
5. [프로젝트 구조 한눈에 보기](#5-프로젝트-구조-한눈에-보기)
6. [이벤트 설계: common-events 모듈](#6-이벤트-설계-common-events-모듈)
7. [시나리오별 전체 실행 흐름 (코드 + 실제 실행 결과)](#7-시나리오별-전체-실행-흐름-코드--실제-실행-결과)
8. [핵심 실무 패턴 딥다이브](#8-핵심-실무-패턴-딥다이브)
9. [Kafka UI로 직접 눈으로 확인하기](#9-kafka-ui로-직접-눈으로-확인하기)
10. [실행 방법 총정리 (치트시트)](#10-실행-방법-총정리-치트시트)
11. [실전에서 겪은 트러블슈팅 노트](#11-실전에서-겪은-트러블슈팅-노트)
12. [다음 단계: 더 깊이 파고들기](#12-다음-단계-더-깊이-파고들기)

---

## 1. 이 프로젝트가 푸는 문제

이커머스에서 주문 하나가 처리되려면 보통 이런 단계를 거칩니다.

```
주문 생성 → 재고 확인/차감 → 결제 → (성공/실패에 따라) 알림 발송, 주문 상태 확정
```

이걸 **하나의 서비스, 하나의 트랜잭션**으로 처리하면 쉽지만, 실무에서는 보통 각 단계가
서로 다른 팀이 소유한 **서로 다른 서비스**(주문팀, 재고팀, 결제팀, 알림팀)로 나뉩니다.
이때 서비스 간 호출을 REST로 동기 처리하면:

- 결제 서비스가 느려지면 주문 서비스도 같이 느려진다 (장애 전파)
- 재고 서비스가 잠깐 죽으면 주문 자체가 실패한다 (강한 결합)
- 알림 서비스 하나 추가하려면 주문 서비스 코드를 고쳐야 한다 (확장성 저하)

이런 문제를 완화하기 위해 실무에서 매우 흔하게 쓰는 방식이 **Kafka를 통한 이벤트 기반 통신**입니다.
서비스들은 서로를 직접 호출하지 않고, "무슨 일이 일어났다"는 이벤트를 Kafka에 발행(publish)하고,
관심 있는 서비스가 그 이벤트를 구독(subscribe)해서 자기 할 일을 합니다. 이 프로젝트는 그 패턴을
**4개의 진짜 동작하는 Spring Boot 서비스**로 구현한 것입니다.

---

## 2. Kafka 핵심 개념 먼저 이해하기

코드를 보기 전에 Kafka 용어부터 정리합니다. 이미 안다면 [3장](#3-아키텍처-choreography-saga)으로 건너뛰세요.

### 2.1 브로커, 토픽, 파티션

- **브로커(Broker)**: Kafka 서버 프로세스 한 대. 이 프로젝트에서는 `docker-compose.yml`의 `kafka`
  컨테이너 하나가 브로커 역할을 합니다.
- **토픽(Topic)**: 메시지를 담는 이름표 붙은 우편함. 예: `order.created`, `payment.completed`.
  카테고리별로 메시지를 분리하는 단위입니다.
- **파티션(Partition)**: 토픽은 내부적으로 1개 이상의 파티션으로 나뉩니다. 파티션은 **순서가
  보장되는 로그(append-only log)**입니다. 이 프로젝트는 모든 토픽을 파티션 1개로 단순화했지만,
  실무에서는 처리량을 늘리기 위해 파티션을 여러 개(예: 12개) 두고 병렬로 컨슈머를 늘립니다.

```
토픽: order.created
┌─────────────────────────────────────────┐
│ 파티션 0: [msg0][msg1][msg2][msg3]...    │  ← 순서 보장은 "파티션 안에서만"
└─────────────────────────────────────────┘
```

### 2.2 프로듀서와 컨슈머, 컨슈머 그룹

- **프로듀서(Producer)**: 메시지를 토픽에 쓰는 쪽. 이 프로젝트에서는 `OrderEventProducer`,
  `InventoryEventProducer`, `PaymentEventProducer` 클래스들.
- **컨슈머(Consumer)**: 토픽을 구독해서 메시지를 읽는 쪽. `@KafkaListener`가 붙은 메서드들.
- **컨슈머 그룹(Consumer Group)**: 같은 `group-id`를 가진 컨슈머들의 묶음. Kafka는 파티션을
  그룹 내 컨슈머들에게 **하나씩 배타적으로** 할당합니다. 그룹을 나누는 이유는:
  - 서비스마다 독립적으로 메시지를 읽기 위해서 (order-service와 inventory-service가 같은
    이벤트를 각자의 속도로 읽어야 하므로 서로 다른 그룹이어야 함)
  - 같은 서비스를 여러 인스턴스로 늘렸을 때(수평 확장) 파티션을 나눠 가지며 부하를 분산하기 위해서

이 프로젝트에서는 서비스당 하나의 `group-id`를 사용합니다 (`order-service`, `inventory-service`,
`payment-service`, `notification-service`). 예를 들어 `inventory.failed` 토픽은
`order-service`와 `notification-service` **둘 다** 구독하는데, group-id가 다르기 때문에
같은 메시지를 두 그룹이 **각자 독립적으로 전부** 받습니다 (이게 Kafka가 pub/sub을 구현하는 방식).

### 2.3 메시지 키와 파티셔닝, 순서 보장

프로듀서가 메시지를 보낼 때 key와 value를 함께 보낼 수 있습니다. Kafka는 **같은 key는 항상 같은
파티션**으로 보냅니다 (`hash(key) % 파티션수`). 이 프로젝트의 모든 producer 코드를 보면:

```java
// order-service/.../kafka/OrderEventProducer.java
kafkaTemplate.send(Topics.ORDER_CREATED, event.orderId(), event);
//                                        ^^^^^^^^^^^^^^^ 메시지 key = orderId
```

**왜 orderId를 key로 쓰는가?** 한 주문에 대한 이벤트가 여러 개 발생할 수 있는데
(`order.created` → `inventory.reserved` → `payment.completed`), 만약 이 이벤트들이 서로
다른 파티션에 흩어지면 컨슈머가 처리하는 순서가 뒤바뀔 수 있습니다. key를 orderId로 고정하면
"같은 주문에 대한 이벤트는 항상 같은 파티션에, 발행한 순서 그대로" 쌓이는 것이 보장됩니다
(파티션이 여러 개인 실무 환경에서 특히 중요한 개념입니다. 이 프로젝트는 파티션이 1개뿐이라
사실 항상 순서가 보장되지만, 실무 규모에서는 이 key 설계가 핵심입니다).

### 2.4 오프셋과 at-least-once 전달

컨슈머는 파티션의 어디까지 읽었는지를 **오프셋(offset)**이라는 숫자로 기록합니다. Kafka는 기본적으로
**at-least-once**(최소 한 번) 전달을 보장합니다: 컨슈머가 메시지를 처리하다 죽거나 재시작되면,
마지막으로 커밋된 오프셋부터 **다시** 읽습니다. 즉 **같은 메시지를 두 번 받을 수 있다**는 뜻입니다.
이게 바로 이 프로젝트에서 [멱등 컨슈머 패턴](#84-멱등-컨슈머)을 넣은 이유입니다.

### 2.5 KRaft (Zookeeper 없는 Kafka)

과거 Kafka는 클러스터 메타데이터(어떤 브로커가 리더인지 등)를 관리하기 위해 별도의 **Zookeeper**
프로세스가 필요했습니다. 최신 Kafka(4.x)는 **KRaft**라는 자체 합의 프로토콜로 Zookeeper 없이
동작합니다 — 브로커 자신이 `controller` 역할도 겸합니다. 이 프로젝트의 `docker-compose.yml`이
컨테이너 하나만으로 Kafka를 띄울 수 있는 이유입니다. (자세한 설정은 [4장](#4-로컬-인프라-docker-composeyml-완전-분석)에서)

---

## 3. 아키텍처: Choreography Saga

### 3.1 왜 이런 흐름인가

```
                         ┌──────────────────┐
   POST /orders   ─────► │  order-service    │  (포트 8081)
                         │  REST + Producer  │
                         │  + Saga listener  │
                         └─────────┬─────────┘
                                   │ order.created (key=orderId)
                                   ▼
                         ┌──────────────────┐
                         │ inventory-service │  (포트 8082)
                         │ 재고 확인/차감/복원 │
                         └─────────┬─────────┘
                     inventory.reserved / inventory.failed
                                   ▼
                         ┌──────────────────┐
                         │  payment-service  │  (포트 8083)
                         │  결제 시뮬레이션    │
                         └─────────┬─────────┘
                     payment.completed / payment.failed
                                   │
                 ┌─────────────────┼─────────────────────┐
                 ▼                 ▼                     ▼
        order-service       notification-service   inventory-service
      (주문 상태 갱신)      (알림 로그 발송)      (결제 실패 시 재고 복원)
```

### 3.2 Choreography vs Orchestration

여러 서비스에 걸친 하나의 비즈니스 트랜잭션을 **Saga**라고 부릅니다. Saga를 구현하는 방법은 크게 둘입니다.

| 방식 | 설명 | 이 프로젝트 |
|---|---|---|
| **Orchestration** | 중앙 오케스트레이터가 "재고 서비스야 확인해", "결제 서비스야 결제해"라고 순서대로 지시 | 사용 안 함 |
| **Choreography** | 중앙 지휘자 없이, 각 서비스가 이벤트를 구독해 스스로 다음 행동을 결정 | **이 프로젝트가 사용하는 방식** |

Choreography는 서비스 간 결합도가 가장 낮다는 장점이 있지만, 전체 흐름이 한 곳에 안 보이고
여러 서비스 코드를 다 봐야 전체 그림이 그려진다는 단점이 있습니다. 이 문서의 [7장](#7-시나리오별-전체-실행-흐름-코드--실제-실행-결과)이
바로 그 "흩어진 흐름"을 한 시나리오씩 따라가며 보여주는 파트입니다.

### 3.3 보상 트랜잭션(Compensating Transaction)이란

일반 DB 트랜잭션은 실패하면 `ROLLBACK`으로 되돌리면 그만입니다. 하지만 여러 서비스에 걸친 Saga는
"전체를 되돌리는 한 방"이 없습니다. 대신 각 단계가 실패했을 때 **이전 단계의 효과를 취소하는 별도의
트랜잭션**을 실행합니다. 이 프로젝트에서는:

- 재고를 미리 차감(`inventory.reserved`)한 뒤 결제를 시도했는데
- 결제가 실패(`payment.failed`)하면
- `inventory-service`가 그 이벤트를 구독해 **차감했던 재고를 다시 더해줍니다** (복원)

이게 보상 트랜잭션입니다. 코드는 [7.3절](#73-시나리오-c-결제-실패--보상-트랜잭션)에서 자세히 봅니다.

---

## 4. 로컬 인프라: docker-compose.yml 완전 분석

전체 파일(`/Users/wond/workspace/kafka/docker-compose.yml`):

```yaml
services:
  kafka:
    image: apache/kafka:4.3.1
    container_name: kafka
    ports:
      - "9092:9092"
      - "9094:9094"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_HOST://0.0.0.0:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 10

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8090:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    depends_on:
      kafka:
        condition: service_healthy

volumes:
  kafka-data:
```

### 4.1 KRaft 설정 한 줄씩

- `KAFKA_PROCESS_ROLES: broker,controller` — 이 브로커 하나가 **데이터 처리(broker)**와
  **메타데이터 합의(controller)** 역할을 동시에 맡습니다 (Zookeeper 대신).
- `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093` — "노드 ID 1번이 controller이고,
  `kafka:9093`으로 접속하면 된다"는 의미. 운영 환경에서는 보통 3개 이상의 노드를 투표권자로 등록해
  고가용성을 확보합니다. 여기서는 학습용 단일 노드라 1개뿐입니다.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` — 프로듀서가 존재하지 않는 토픽에 메시지를 보내면
  자동으로 토픽이 생성됩니다. (운영 환경에서는 보통 끄고 Terraform 등으로 토픽을 명시적으로 관리합니다.)

### 4.2 리스너가 3개인 이유

Kafka 브로커는 여러 개의 **리스너(listener)**를 동시에 열어둘 수 있습니다. 이 프로젝트가 3개를
쓰는 이유는 "누가 접속하느냐"에 따라 브로커에 도달하는 주소가 다르기 때문입니다.

| 리스너 | 포트 | 용도 |
|---|---|---|
| `CONTROLLER` | 9093 | KRaft 합의용 (브로커 간 내부 통신) |
| `PLAINTEXT` | 9092 | **Docker 네트워크 내부**에서의 접속용 (예: `kafka-ui` 컨테이너가 `kafka:9092`로 접속) |
| `PLAINTEXT_HOST` | 9094 | **호스트 머신(내 맥북)**에서 접속용 — Spring Boot 서비스들을 컨테이너 밖에서
  `./gradlew bootRun`으로 직접 띄웠기 때문에, 이 서비스들은 `localhost:9094`로 접속합니다 |

`KAFKA_ADVERTISED_LISTENERS`가 중요한 이유: 클라이언트가 브로커에 처음 접속하면, 브로커는
"진짜 데이터를 주고받을 땐 이 주소로 다시 연결해"라고 **advertised 주소**를 알려줍니다. 이게
컨테이너 내부 주소(`kafka:9092`)만 있으면 호스트에서 접속한 클라이언트는 `kafka`라는 호스트명을
못 찾아서 실패합니다. 그래서 `PLAINTEXT_HOST://localhost:9094`를 추가로 광고하는 것입니다.

### 4.3 실전에서 겪은 이슈: 9094 포트 노출 누락

이 프로젝트를 만들면서 실제로 겪은 문제를 그대로 남겨둡니다 (좋은 학습 사례이기 때문입니다).

1. 처음에 `ports:` 에 `"9092:9092"`만 적었습니다.
2. 각 Spring Boot 서비스를 띄우니 컨슈머 로그에 계속 이런 경고가 떴습니다.

   ```
   WARN ... [Consumer clientId=consumer-inventory-service-1, groupId=inventory-service]
   Connection to node -1 (localhost/127.0.0.1:9094) could not be established. Node may not be available.
   ```

3. 원인 파악: 브로커 **내부**에서는 9094 포트로 리스닝하고 있었지만(`KAFKA_LISTENERS`),
   docker-compose가 그 포트를 **호스트로 전달(publish)하지 않았기** 때문에 맥북에서
   `localhost:9094`로 접속을 시도해도 아무도 응답하지 않았습니다.
4. 해결: `ports:`에 `"9094:9094"`를 추가하고 `docker compose up -d`로 컨테이너를 재생성.

**교훈**: docker-compose의 `ports: HOST:CONTAINER` 매핑과, 컨테이너 안에서 애플리케이션이
리스닝하는 포트는 **별개의 개념**입니다. 컨테이너 안에서 열려 있어도 `ports`에 명시하지 않으면
호스트에서는 절대 접근할 수 없습니다.

### 4.4 Kafka UI

`provectuslabs/kafka-ui`는 브라우저에서 토픽/메시지/컨슈머 그룹/파티션 상태를 볼 수 있는 무료
오픈소스 도구입니다. 실무에서도 로컬/스테이징 환경 디버깅용으로 정말 많이 씁니다. 사용법은
[9장](#9-kafka-ui로-직접-눈으로-확인하기)에서 스크린별로 안내합니다.

---

## 5. 프로젝트 구조 한눈에 보기

```
kafka/
├── settings.gradle                # 5개 모듈을 묶는 진입점
├── build.gradle                   # 공통 설정: Java 21 toolchain, Spring Boot 4.1 BOM
├── docker-compose.yml             # Kafka(KRaft) + Kafka UI
├── ARCHITECTURE.md                # 아키텍처 요약 문서
├── LEARNING_GUIDE.md              # 이 문서
│
├── common-events/                 # 순수 Java 라이브러리 (Spring 의존성 없음)
│   └── .../events/
│       ├── DomainEvent.java           # 모든 이벤트가 구현하는 공통 인터페이스
│       ├── OrderCreatedEvent.java
│       ├── InventoryReservedEvent.java
│       ├── InventoryFailedEvent.java
│       ├── PaymentCompletedEvent.java
│       ├── PaymentFailedEvent.java
│       └── Topics.java                # 토픽 이름 상수
│
├── order-service/          (port 8081)   REST API, 주문 생성/조회, Saga 최종 상태 갱신
├── inventory-service/      (port 8082)   재고 확인/차감/복원
├── payment-service/        (port 8083)   결제 시뮬레이션
└── notification-service/   (port 8084)   알림 로그 시뮬레이션
```

각 서비스 내부는 동일한 패키지 구조를 따릅니다 (한 서비스를 이해하면 나머지도 쉽습니다):

```
com.example.kafkasaga.<service>/
├── <Service>Application.java   # @SpringBootApplication 진입점
├── domain/                     # JPA 엔티티 (Order, ProductStock, Payment...)
├── repository/                 # Spring Data JPA 레포지토리
├── kafka/                       # Producer, Listener(Consumer), 멱등 처리용 ProcessedEvent
├── config/                      # KafkaErrorHandlingConfig (재시도+DLQ 공통 설정)
└── web/                          # (order-service만) REST 컨트롤러
```

---

## 6. 이벤트 설계: common-events 모듈

모든 서비스가 이 모듈을 `implementation project(':common-events')`로 의존합니다. 서비스 간에
"이벤트가 어떤 모양인지"를 공유하는 계약(contract) 역할을 합니다.

```java
// common-events/.../events/DomainEvent.java
public interface DomainEvent {
    String eventId();
    String orderId();
    Instant occurredAt();
}
```

모든 이벤트가 공통으로 갖는 필드를 인터페이스로 뽑아둔 이유는, 각 서비스의 멱등 처리 로직
(`withIdempotency(DomainEvent event, Runnable action)`)이 **구체 타입을 몰라도** `eventId()`
하나로 중복 여부를 판단할 수 있게 하기 위해서입니다.

예시로 `OrderCreatedEvent` (Java 17+ `record` 문법 — 필드, 생성자, getter, equals/hashCode를
자동 생성해주는 불변 데이터 클래스입니다):

```java
// common-events/.../events/OrderCreatedEvent.java
public record OrderCreatedEvent(
        String eventId,      // 이 이벤트 자체의 고유 ID (멱등성 체크용)
        String orderId,      // 어떤 주문에 대한 이벤트인지 (파티셔닝 key로도 사용)
        String productId,
        int quantity,
        long amount,         // 주문 총액 (원)
        Instant occurredAt   // 이벤트 발생 시각
) implements DomainEvent {
}
```

나머지 4개 이벤트(`InventoryReservedEvent`, `InventoryFailedEvent`, `PaymentCompletedEvent`,
`PaymentFailedEvent`)도 같은 패턴이며, 각자 다음 단계 서비스가 판단에 필요한 필드만 담고 있습니다.
예를 들어 `PaymentFailedEvent`는 `productId`와 `quantity`를 갖고 있는데, 이는
`inventory-service`가 이 이벤트를 받아 **재고를 복원**할 때 "무엇을 얼마나 복원해야 하는지"
알아야 하기 때문입니다.

토픽 이름은 문자열을 서비스마다 따로 적으면 오타 위험이 있으므로 상수로 모아뒀습니다.

```java
// common-events/.../events/Topics.java
public final class Topics {
    public static final String ORDER_CREATED = "order.created";
    public static final String INVENTORY_RESERVED = "inventory.reserved";
    public static final String INVENTORY_FAILED = "inventory.failed";
    public static final String PAYMENT_COMPLETED = "payment.completed";
    public static final String PAYMENT_FAILED = "payment.failed";
}
```

---

## 7. 시나리오별 전체 실행 흐름 (코드 + 실제 실행 결과)

아래 시나리오들은 실제로 로컬에서 4개 서비스를 띄워 직접 실행하고 검증한 결과입니다
(아래 curl 결과와 로그는 지어낸 예시가 아니라 실제 실행 출력입니다).

### 7.1 시나리오 A: 정상 결제 흐름

**요청**

```bash
curl -X POST localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"productId":"P001","quantity":2}'
```

**즉시 응답** (order-service가 주문을 PENDING으로 만들고 바로 응답 — 뒤의 재고/결제 처리는
비동기로 진행됩니다):

```json
{"orderId":"3b5468a8-f2db-4315-9e67-6283cf4ec02b","productId":"P001","quantity":2,
 "amount":20000,"status":"PENDING","statusReason":"주문이 접수되었습니다.", ...}
```

**코드 흐름 1 — order-service가 주문을 저장하고 이벤트를 발행**

```java
// order-service/.../service/OrderService.java
@Transactional
public Order createOrder(String productId, int quantity) {
    String orderId = UUID.randomUUID().toString();
    long amount = UNIT_PRICE * quantity;              // 데모용 고정 단가 10,000원

    Order order = new Order(orderId, productId, quantity, amount);
    orderRepository.save(order);                        // ① DB에 PENDING 상태로 저장

    OrderCreatedEvent event = new OrderCreatedEvent(
            UUID.randomUUID().toString(), orderId, productId, quantity, amount, Instant.now());
    orderEventProducer.publishOrderCreated(event);       // ② order.created 토픽에 발행

    return order;
}
```

**코드 흐름 2 — inventory-service가 재고를 확인/차감**

```java
// inventory-service/.../kafka/InventoryEventListener.java
@KafkaListener(topics = Topics.ORDER_CREATED, groupId = "inventory-service")
@Transactional
public void onOrderCreated(OrderCreatedEvent event) {
    withIdempotency(event, () -> {
        Optional<ProductStock> stock = productStockRepository.findById(event.productId());

        if (stock.isEmpty()) { publishFailed(event, "존재하지 않는 상품: ..."); return; }
        if (!stock.get().hasEnoughStock(event.quantity())) {
            publishFailed(event, "재고 부족 (요청 %d / 보유 %d)".formatted(...));
            return;
        }

        stock.get().decrease(event.quantity());          // ③ 재고 차감
        productStockRepository.save(stock.get());

        var reserved = new InventoryReservedEvent(...);
        inventoryEventProducer.publishReserved(reserved); // ④ inventory.reserved 발행
    });
}
```

재고 시드 데이터는 `StockDataInitializer`에서 앱 시작 시 채워집니다: `P001=100개`,
`P002=3개`(재고 부족 시나리오 재현용), `P003=1000개`.

**코드 흐름 3 — payment-service가 결제를 시뮬레이션**

```java
// payment-service/.../kafka/PaymentEventListener.java
private static final long DECLINE_THRESHOLD = 500_000L;   // 이 금액 넘으면 결제 거절 시뮬레이션

@KafkaListener(topics = Topics.INVENTORY_RESERVED, groupId = "payment-service")
@Transactional
public void onInventoryReserved(InventoryReservedEvent event) {
    boolean approved = event.amount() <= DECLINE_THRESHOLD;   // ⑤ 결정론적 성공/실패 규칙
    paymentRepository.save(new Payment(event.orderId(), event.amount(), approved, reason));

    if (approved) {
        paymentEventProducer.publishCompleted(new PaymentCompletedEvent(...)); // ⑥
    } else {
        paymentEventProducer.publishFailed(new PaymentFailedEvent(...));
    }
}
```

이 예제는 20,000원짜리 주문이라 500,000원 임계값을 넘지 않으므로 **결제 승인**됩니다.

**코드 흐름 4 — order-service가 최종 상태를 확정**

```java
// order-service/.../kafka/OrderSagaEventListener.java
@KafkaListener(topics = Topics.PAYMENT_COMPLETED, groupId = "order-service")
@Transactional
public void onPaymentCompleted(PaymentCompletedEvent event) {
    withIdempotency(event, () -> {
        Order order = findOrder(event.orderId());
        order.confirm();                                    // ⑦ PENDING -> CONFIRMED
    });
}
```

**3초 뒤 재조회 결과** (실제 실행 결과):

```bash
curl localhost:8081/orders/3b5468a8-f2db-4315-9e67-6283cf4ec02b
```
```json
{"orderId":"3b5468a8-...","status":"CONFIRMED",
 "statusReason":"결제가 완료되어 주문이 확정되었습니다.", ...}
```

동시에 `notification-service`도 같은 `payment.completed` 이벤트를 (다른 group-id로) 받아서
알림을 로그로 남깁니다:

```
[알림 발송] orderId=3b5468a8-... | 주문 확정 안내 | 결제가 완료되어 주문이 확정되었습니다. (20000원)
```

**전체 순서 정리**

```
POST /orders
  └─ order-service: Order(PENDING) 저장 → order.created 발행
       └─ inventory-service: 재고 차감 → inventory.reserved 발행
            └─ payment-service: 결제 승인 → payment.completed 발행
                 ├─ order-service: Order → CONFIRMED
                 └─ notification-service: "주문 확정" 알림 로그
```

### 7.2 시나리오 B: 재고 부족

```bash
curl -X POST localhost:8081/orders -d '{"productId":"P002","quantity":10}'
# P002는 재고가 3개뿐 (StockDataInitializer 참고)
```

`inventory-service`의 `hasEnoughStock(10)`이 `false`를 반환 → `InventoryFailedEvent` 발행
→ `order-service`가 `CANCELLED`로 전환, `notification-service`가 취소 알림을 남깁니다.

**실제 실행 결과**:
```json
{"orderId":"e1b8cad5-...","status":"CANCELLED",
 "statusReason":"재고 부족: 재고 부족 (요청 10 / 보유 3)", ...}
```

이 시나리오에서는 결제 단계까지 가지도 않습니다 — Choreography의 장점 중 하나로, 각 단계가
자기 조건을 만족하지 못하면 뒤 단계를 아예 트리거하지 않습니다.

### 7.3 시나리오 C: 결제 실패 + 보상 트랜잭션

```bash
curl -X POST localhost:8081/orders -d '{"productId":"P003","quantity":60}'
# 단가 10,000원 * 60개 = 600,000원 > DECLINE_THRESHOLD(500,000원) → 결제 거절
```

**실제 실행 결과**:
```json
{"orderId":"9b396b2c-...","amount":600000,"status":"CANCELLED",
 "statusReason":"결제 실패: 결제 한도 초과 (금액 600000원)", ...}
```

여기서 중요한 부분은 **재고가 이미 차감된 상태**였다는 것입니다 (payment 단계 전에
inventory-service가 먼저 60개를 차감했었음). 결제가 실패하면 그 차감을 되돌려야 하는데,
이걸 처리하는 코드가 바로 보상 트랜잭션입니다.

```java
// inventory-service/.../kafka/InventoryEventListener.java
/** 결제 실패에 대한 보상 트랜잭션: 확보했던 재고를 복원한다. */
@KafkaListener(topics = Topics.PAYMENT_FAILED, groupId = "inventory-service")
@Transactional
public void onPaymentFailed(PaymentFailedEvent event) {
    withIdempotency(event, () -> {
        productStockRepository.findById(event.productId()).ifPresent(stock -> {
            stock.restore(event.quantity());              // 차감했던 수량만큼 되돌림
            productStockRepository.save(stock);
        });
    });
}
```

**실제 로그로 확인된 복원**:
```
14:05:36.445 Published inventory.reserved for orderId=9b396b2c-...
14:05:36.475 Restored 60 units of P003 for orderId=9b396b2c-... (payment failed)
```

즉 `inventory-service`는 **같은 주문(9b396b2c...)에 대해 두 개의 서로 다른 이벤트**를 순서대로
처리한 것입니다: 먼저 `order.created`를 받아 차감, 나중에 `payment.failed`를 받아 복원.
한 서비스가 여러 토픽을 구독하며 자기 상태를 스스로 관리하는 것이 Choreography의 핵심입니다.

### 7.4 시나리오 D: 컨슈머 에러 + DLQ (재시도 → Dead Letter)

이건 "정상 비즈니스 시나리오"가 아니라, **일부러 고장 난 메시지**를 넣어서 에러 처리 파이프라인이
실제로 동작하는지 검증하는 실습입니다.

**Kafka 컨테이너에 직접 접속해서, JSON이 아닌 이상한 메시지를 `order.created` 토픽에 발행:**

```bash
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic order.created <<< 'this-is-not-valid-json'
```

`inventory-service`가 이 메시지를 읽으려고 하면 JSON 파싱에 실패합니다. 이때 동작하는 파이프라인:

1. **`ErrorHandlingDeserializer`**(application.yml에 설정)가 역직렬화 실패를 예외로 감싸서
   컨슈머 스레드가 죽지 않고 계속 살아있게 해줍니다. (이게 없으면 "poison pill" 메시지 하나가
   전체 컨슈머를 영구적으로 멈춰버릴 수 있습니다 — 실무에서 흔한 장애 원인 중 하나입니다.)
2. **`DefaultErrorHandler`**(`KafkaErrorHandlingConfig`)가 예외를 받아 재시도 여부를 판단합니다.
   역직렬화 예외는 "다시 시도해도 절대 성공할 수 없는" 종류이므로 즉시 recover 단계로 넘어갑니다
   (일반 비즈니스 예외라면 지수 백오프로 1초→2초→4초 재시도).
3. **`DeadLetterPublishingRecoverer`**가 원본 메시지를 `order.created-dlt`라는 별도 토픽으로
   그대로 옮겨 담습니다.

**실제 로그**:
```
WARN DeadLetterPublishingRecoverer - Destination resolver returned non-existent partition
     order.created-dlt-0, KafkaProducer will determine partition to use for this topic
```

**실제로 생성된 토픽 확인**:
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
# ... order.created  order.created-dlt  ...
```

**DLT 토픽 안의 내용 확인** (원본 바이트가 그대로 들어있음):
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order.created-dlt --from-beginning --max-messages 1
# "dGhpcy1pcy1ub3QtdmFsaWQtanNvbg==" (= "this-is-not-valid-json"의 base64)
```

> **주의**: `DeadLetterPublishingRecoverer`의 기본 명명 규칙은 `<원본토픽>-dlt`입니다
> (일부 오래된 문서/튜토리얼에는 `<토픽>.DLT`로 나오는데, 이 프로젝트에서 실제로 확인한
> spring-kafka 4.1 기준 실제 동작은 `-dlt`(소문자, 하이픈)였습니다 — **문서보다 실제 동작을
> 믿고 직접 확인하는 습관**이 실무에서 중요합니다.)

이 DLT 토픽은 운영에서는 보통 별도 모니터링/재처리 배치가 구독해서 "처리 실패한 메시지들"을
사람이 검토하거나 재발행하는 용도로 씁니다.

---

## 8. 핵심 실무 패턴 딥다이브

### 8.1 메시지 키 파티셔닝

[2.3절](#23-메시지-키와-파티셔닝-순서-보장) 참고. 모든 producer가 `orderId`를 key로 사용합니다.

### 8.2 프로듀서 신뢰성 (idempotence, acks=all)

모든 서비스의 `application.yml`에 다음이 있습니다.

```yaml
spring:
  kafka:
    producer:
      acks: all
      properties:
        enable.idempotence: true
```

- `acks: all` — 프로듀서가 "저장 완료" 응답을 받으려면, 리더 브로커뿐 아니라 **모든 in-sync
  replica**가 메시지를 받았다는 확인이 필요합니다. (`acks: 1`은 리더만 받으면 OK라 더 빠르지만
  리더가 죽으면 유실 가능. `acks: 0`은 응답도 안 기다림 — 가장 빠르지만 가장 위험합니다.)
- `enable.idempotence: true` — 네트워크 문제로 프로듀서가 같은 메시지를 재전송해도, 브로커가
  중복 저장을 막아줍니다 (프로듀서 재시도로 인한 **중복 생성**을 방지 — 컨슈머 쪽 멱등성과는
  다른 레이어의 이야기입니다).

### 8.3 재시도 + DLQ

```java
// 예: order-service/.../config/KafkaErrorHandlingConfig.java
@Bean
public DefaultErrorHandler kafkaErrorHandler(KafkaOperations<Object, Object> kafkaOperations) {
    var recoverer = new DeadLetterPublishingRecoverer(kafkaOperations);

    var backOff = new ExponentialBackOff(1_000L, 2.0);   // 1초 시작, 2배씩 증가
    backOff.setMaxElapsedTime(7_000L);                    // 총 7초 넘으면 포기 (약 1s+2s+4s = 3회)

    return new DefaultErrorHandler(recoverer, backOff);
}
```

이 `DefaultErrorHandler` 빈은 Spring Boot가 자동 설정하는 `ConcurrentKafkaListenerContainerFactory`에
**자동으로 연결**됩니다 (별도 배선 코드가 필요 없습니다 — Spring Boot가 `CommonErrorHandler` 타입의
빈을 찾아서 꽂아줍니다). `application.yml`의 `ErrorHandlingDeserializer` 설정과 함께 동작해서,
역직렬화 실패든 비즈니스 로직 예외든 같은 파이프라인(재시도 → 실패 시 DLQ)을 탑니다.

```yaml
# 모든 서비스 application.yml 공통
spring:
  kafka:
    consumer:
      key-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.key.delegate.class: org.apache.kafka.common.serialization.StringDeserializer
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.trusted.packages: com.example.kafkasaga.events
```

`ErrorHandlingDeserializer`는 실제 역직렬화를 `delegate.class`(여기선 `JsonDeserializer`)에게
맡기고, 실패하면 예외를 예쁘게 감싸서 위(`DefaultErrorHandler`)로 넘겨주는 래퍼입니다.
`spring.json.trusted.packages`는 JSON의 `__TypeId__` 헤더를 보고 아무 클래스나 역직렬화하지
않도록 패키지를 화이트리스트로 제한하는 보안 설정입니다 (역직렬화 가젯 공격 방지).

### 8.4 멱등 컨슈머

[2.4절](#24-오프셋과-at-least-once-전달)에서 설명했듯 Kafka는 메시지를 중복으로 전달할 수
있습니다. 이 프로젝트의 모든 리스너는 처리 전에 "이 이벤트, 나 이미 처리한 적 있나?"를 확인합니다.

```java
// 예: order-service/.../kafka/OrderSagaEventListener.java
private void withIdempotency(DomainEvent event, Runnable action) {
    if (processedEventRepository.existsById(event.eventId())) {
        log.info("Skip already-processed event {} ...", event.eventId());
        return;                                 // 이미 처리했으면 아무것도 안 하고 종료
    }
    action.run();                                // 실제 비즈니스 로직 실행
    processedEventRepository.save(new ProcessedEvent(event.eventId()));  // 처리 완료 기록
}
```

`ProcessedEvent`는 `eventId`를 PK로 갖는 아주 단순한 엔티티이고, 각 서비스마다 자기 H2 DB에
`processed_events` 테이블로 존재합니다. 핵심은 **비즈니스 로직 실행과 "처리 완료 기록"이 같은
`@Transactional` 안에서 함께 커밋**된다는 점입니다 — 둘 중 하나만 성공하는 상황(예: 재고는
차감했는데 기록은 실패)을 막아줍니다.

### 8.5 컨슈머 그룹

[2.2절](#22-프로듀서와-컨슈머-컨슈머-그룹) 참고. 각 서비스가 자신의 이름을 `group-id`로
사용합니다 (`order-service`, `inventory-service`, `payment-service`, `notification-service`).

---

## 9. Kafka UI로 직접 눈으로 확인하기

Kafka UI는 브라우저 기반 관리 도구입니다. 아래 순서대로 따라 해보세요.

### 9.1 접속하기

1. Kafka가 이미 떠 있어야 합니다: `docker compose up -d`
2. 브라우저에서 **http://localhost:8090** 접속
3. 좌측 사이드바에 클러스터 이름 `local`이 보이면 정상 연결된 것입니다.

### 9.2 토픽 목록/메시지 확인하기

1. 좌측 메뉴 **Topics** 클릭 → 이 프로젝트가 만든 토픽들이 보입니다:
   `order.created`, `inventory.reserved`, `inventory.failed`, `payment.completed`,
   `payment.failed` (그리고 DLQ 실습을 했다면 `order.created-dlt`도 보입니다).
2. 토픽 이름을 클릭하면 상단에 **Overview / Messages / Consumers / ...** 탭이 있습니다.
   - **Overview**: 파티션 개수, 메시지 총량(리텐션에 따라 다름), 복제본 수 등
   - **Messages** 탭: 실제로 쌓인 메시지를 하나씩 볼 수 있습니다. 우측 상단에서
     `Offset`/`Timestamp` 기준으로 필터링할 수 있고, 메시지를 클릭하면 **Key, Value, Headers**가
     JSON으로 예쁘게 펼쳐집니다.
   - Headers를 펼쳐보면 `__TypeId__` 헤더에 `com.example.kafkasaga.events.OrderCreatedEvent`
     처럼 이 메시지가 어떤 자바 클래스로 만들어졌는지가 그대로 보입니다 (JsonSerializer가
     자동으로 붙인 것 — [8.3절](#83-재시도--dlq)에서 설명한 타입 정보가 실제로 여기서 옵니다).
   - Key 컬럼에서 여러 메시지의 key(=orderId)가 같은 주문 흐름을 어떻게 여러 토픽에 걸쳐
     이어지는지 눈으로 직접 추적할 수 있습니다.

### 9.3 컨슈머 그룹 상태 확인하기

1. 좌측 메뉴 **Consumers** 클릭 → `order-service`, `inventory-service`, `payment-service`,
   `notification-service` 4개 그룹이 보입니다.
2. 그룹을 클릭하면 그 그룹이 구독 중인 토픽별 **현재 오프셋(current offset)**, **끝 오프셋
   (end offset)**, 그리고 **Lag**(밀린 메시지 수)이 보입니다. Lag이 0이면 실시간으로 다
   따라잡은 것이고, 숫자가 계속 쌓이면 컨슈머가 처리 속도를 못 따라가고 있다는 신호입니다
   (실무에서 장애/성능 진단의 1번 지표입니다).

### 9.4 DLQ 실습 결과 확인하기

[7.4절](#74-시나리오-d-컨슈머-에러--dlq-재시도--dead-letter)의 실습을 했다면:

1. **Topics** 목록에 `order.created-dlt`가 새로 생겨 있는지 확인
2. 그 토픽의 **Messages** 탭을 열면 우리가 넣은 깨진 메시지(`this-is-not-valid-json`)의 원본
   바이트가 그대로 들어있는 걸 볼 수 있습니다. Value가 사람이 읽을 수 있는 JSON이 아니라
   깨진 문자열/바이너리로 보이는 게 정상입니다 (역직렬화에 실패한 원본을 그대로 보존하기 때문).

### 9.5 토픽을 직접 만들거나 메시지를 보내보고 싶다면

**Topics → Create Topic** 버튼으로 파티션 수/리텐션 등을 바꿔가며 실험할 수 있고,
토픽 상세 화면의 **Messages 탭 → Produce Message** 버튼으로 UI에서 직접 key/value를 입력해
메시지를 발행해볼 수도 있습니다. (터미널로 하는 것과 동일한 실습을 GUI로 할 수 있는 셈입니다.)

---

## 10. 실행 방법 총정리 (치트시트)

```bash
# 1) Kafka + Kafka UI 로컬 기동
cd /Users/wond/workspace/kafka
docker compose up -d
docker compose ps      # kafka, kafka-ui 둘 다 Up(healthy) 확인

# 2) Java 21 지정 (로컬에 17이 없어 21로 진행 — build.gradle 참고)
export JAVA_HOME=/Users/wond/Library/Java/JavaVirtualMachines/ms-21.0.7/Contents/Home

# 3) 4개 서비스를 각각 별도 터미널(또는 백그라운드)에서 기동
./gradlew :order-service:bootRun
./gradlew :inventory-service:bootRun
./gradlew :payment-service:bootRun
./gradlew :notification-service:bootRun

# 4) 정상 흐름 테스트
curl -X POST localhost:8081/orders -H "Content-Type: application/json" \
  -d '{"productId":"P001","quantity":2}'
# 응답의 orderId로 잠시 후 재조회
curl localhost:8081/orders/{orderId}

# 5) 재고 부족 테스트 (P002 재고=3)
curl -X POST localhost:8081/orders -d '{"productId":"P002","quantity":10}'

# 6) 결제 실패 + 보상 트랜잭션 테스트 (10,000원 * 60 = 600,000원 > 임계값 500,000원)
curl -X POST localhost:8081/orders -d '{"productId":"P003","quantity":60}'

# 7) DLQ 실습: 깨진 메시지 주입
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic order.created <<< 'this-is-not-valid-json'
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 8) Kafka UI로 눈으로 확인
open http://localhost:8090

# 9) 전체 빌드/컴파일만 확인하고 싶을 때
./gradlew build -x test
```

각 서비스는 독립된 H2 파일 DB(`./data/<service>`)를 사용하므로, 상태를 초기화하고 싶으면
서비스를 끄고 `data/` 디렉터리를 지운 뒤 다시 켜면 됩니다.

---

## 11. 실전에서 겪은 트러블슈팅 노트

이 프로젝트를 만드는 과정에서 실제로 마주친 문제와 해결 과정입니다. 비슷한 에러를 만났을 때
"검색만 하지 말고 원인을 이해하고 고치는" 연습에 참고하세요.

### 11.1 `Spring Boot 3.x`가 아니라 `Spring Boot 4.1`을 쓰게 된 이유

Spring Boot 3.x는 2026-06-30부로 OSS 지원이 종료되어, 신규 학습 프로젝트로는 최신 안정 버전인
**4.1.0**(Spring Framework 7 기반)을 선택했습니다.

### 11.2 `KafkaTemplate` 빈을 찾을 수 없다는 에러

```
No qualifying bean of type 'org.springframework.kafka.core.KafkaTemplate' available
```

**원인**: Spring Boot 4부터 프레임워크가 여러 개의 작은 모듈로 재구성되면서, Kafka
자동 설정(`KafkaAutoConfiguration`)이 `spring-boot-autoconfigure`에서 빠지고 별도의
`spring-boot-starter-kafka` / `spring-boot-kafka` 모듈로 이동했습니다. 기존 방식대로
`org.springframework.kafka:spring-kafka`만 의존성에 추가하면 **spring-kafka 라이브러리 자체는
받아지지만, Spring Boot가 자동으로 빈을 만들어주는 부분이 빠져** 있었던 것입니다.

**해결**: 4개 서비스의 `build.gradle`에서

```groovy
implementation 'org.springframework.kafka:spring-kafka'          // 이전
implementation 'org.springframework.boot:spring-boot-starter-kafka'  // 수정 후
```

**교훈**: 메이저 버전이 올라가면(3 → 4) 익숙했던 스타터/자동설정 구성이 바뀔 수 있습니다.
에러 메시지의 "빈을 못 찾았다"는 표면적 증상만 보지 말고, 그 빈을 만들어줬어야 할
자동 설정 클래스가 **왜 로드되지 않았는지**(클래스패스에 없는지, 조건이 안 맞는지)를
추적하는 습관이 중요합니다 (`unzip -l <jar>`으로 실제 클래스가 들어있는지 직접 까본 것도
이 과정에서 큰 도움이 됐습니다).

### 11.3 `KafkaTemplate<String, Object>` 대신 `KafkaTemplate<Object, Object>`를 쓴 이유

Spring Boot가 자동 생성하는 `KafkaTemplate` 빈의 제네릭 타입은 `KafkaTemplate<Object, Object>`
입니다. Spring의 의존성 주입은 제네릭 타입까지 정확히 비교하기 때문에, 코드에서
`KafkaTemplate<String, Object>`로 주입받으려 하면 타입 불일치로 빈을 못 찾는 문제가 생길 수
있습니다. 그래서 이 프로젝트의 모든 Producer는 `KafkaTemplate<Object, Object>`로 선언했고,
실제 key 값은 `send(topic, key, value)` 호출 시점에 `String`을 넘겨도 문제없이 동작합니다
(제네릭은 컴파일 타임 체크일 뿐, 런타임 `send` 메서드 시그니처는 `Object`를 받기 때문입니다).

### 11.4 `localhost:9094` 연결 실패 (포트 미노출)

[4.3절](#43-실전에서-겪은-이슈-9094-포트-노출-누락)에서 상세히 다뤘습니다. docker-compose의
`ports` 매핑 누락이 원인이었습니다.

### 11.5 Kafka UI 포트 충돌 (8080 already in use)

`docker compose up -d` 실행 중 다음 에러:

```
Error response from daemon: Ports are not available: exposing port TCP 0.0.0.0:8080 -> 0.0.0.0:0:
listen tcp4 0.0.0.0:8080: bind: address already in use
```

`lsof -i :8080`으로 확인해보니 이 맥북에서 이미 다른 프로젝트의 Java 프로세스가 8080을 쓰고
있었습니다. **해결**: Kafka UI 포트를 호스트 쪽만 `8090:8080`으로 바꿔서 충돌을 피했습니다
(컨테이너 내부 포트는 그대로 8080). **교훈**: 로컬에 여러 프로젝트를 동시에 띄워두는 환경에서는
포트 충돌이 아주 흔하니, 에러 메시지의 포트 번호로 `lsof -i :<port>`부터 확인하는 습관을
들이면 좋습니다.

### 11.6 `DeadLetterPublishingRecoverer`의 실제 DLQ 토픽 명명 규칙

오래된 블로그/문서에는 `<topic>.DLT`(대문자, 점)로 소개된 경우가 많은데, 이 프로젝트에서
spring-kafka 4.1 기준으로 직접 실험해보니 실제로는 `<topic>-dlt`(소문자, 하이픈)이 생성되었습니다.
**교훈**: 라이브러리 메이저 버전이 다르면 기본값이 조용히 바뀔 수 있습니다. 특히 학습 목적이라면
"문서에 이렇게 써 있다"에서 멈추지 말고, 이 프로젝트처럼 **직접 실행해서 실제 생성된 토픽 이름을
`kafka-topics.sh --list`로 확인**하는 것이 가장 정확합니다.

### 11.7 로컬에 Java 17이 없어서 Java 21로 진행

Spring Boot 4.1은 Java 17~26을 지원합니다. `/usr/libexec/java_home -V`로 확인해보니 이
맥북에는 17이 없고 21/23만 설치되어 있어, `build.gradle`의 toolchain을 21로 맞췄습니다.
필요하면 나중에 `sdkman`이나 각 벤더 배포판으로 17을 설치해 그대로 바꿔도 됩니다
(`JavaLanguageVersion.of(17)`로 한 줄만 수정하면 됩니다).

---

## 12. 다음 단계: 더 깊이 파고들기

이 프로젝트는 "기본 + 핵심 실무 패턴"까지만 다뤘습니다. 더 공부하고 싶다면 다음 주제들을
같은 코드베이스 위에 하나씩 추가해보는 것을 추천합니다.

1. **Outbox 패턴**: 지금은 `@Transactional` 메서드 안에서 DB 저장과 Kafka 발행을 함께
   하고 있는데, 엄밀히는 이 둘이 하나의 원자적 트랜잭션이 아닙니다(DB는 커밋됐는데 Kafka
   발행이 실패하는 경우가 이론상 가능). Outbox 테이블에 이벤트를 DB 트랜잭션 안에서 함께
   저장하고, 별도 프로세스(또는 Debezium 같은 CDC)가 그 테이블을 읽어 Kafka로 발행하면
   완전한 원자성을 얻을 수 있습니다.
2. **Kafka Streams / KSQL**: 지금은 단순 pub/sub만 쓰지만, 실시간 집계(예: "최근 5분간
   주문 금액 합계")가 필요하면 Kafka Streams DSL로 스트림 처리를 배워볼 수 있습니다.
3. **Schema Registry (Avro/Protobuf)**: 지금은 JSON + 클래스 헤더로 타입을 맞추고 있는데,
   서비스가 많아지고 이벤트 스키마가 자주 바뀌면 Confluent Schema Registry로 스키마
   호환성(backward/forward compatibility)을 강제하는 것이 표준적인 다음 단계입니다.
4. **파티션을 여러 개로 늘려 병렬 처리 체감하기**: 지금은 파티션 1개라 병렬성이 없습니다.
   토픽 파티션을 3개로 늘리고, 같은 서비스를 2~3개 인스턴스로 띄운 뒤 컨슈머 그룹의
   리밸런싱(rebalancing)이 실제로 어떻게 파티션을 나눠 갖는지 로그로 관찰해보세요.
5. **트랜잭셔널 프로듀서 (Exactly-once)**: `spring.kafka.producer.transaction-id-prefix`를
   설정하고 `@Transactional`과 Kafka 트랜잭션을 묶어, "DB 커밋과 Kafka 발행이 함께 성공하거나
   함께 실패하는" 진짜 exactly-once에 가까운 패턴을 실험해볼 수 있습니다.
6. **Testcontainers 기반 통합 테스트**: `spring-kafka-test`가 이미 테스트 의존성에 들어있으니,
   `@EmbeddedKafka`나 Testcontainers의 Kafka 모듈로 실제 브로커 없이도 CI에서 돌아가는
   통합 테스트를 작성해보세요.

이 문서와 코드를 같이 놓고 한 시나리오씩 직접 실행해보면서 로그와 Kafka UI를 함께 보는 것이
가장 빠르게 감을 잡는 방법입니다. 막히는 부분이 있으면 [11장 트러블슈팅 노트](#11-실전에서-겪은-트러블슈팅-노트)의
방식대로 "표면 에러 → 원인 추적 → 검증"의 흐름을 그대로 따라 해보세요.
