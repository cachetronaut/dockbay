export type JsonPrimitive = string | number | boolean | null;
export type JsonValue =
  | JsonPrimitive
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };

export interface ConvexMutationContext {
  readonly kind: "mutation";
}

export interface ConvexQueryContext {
  readonly kind: "query";
}

export type ConvexOperationKind = "mutation" | "query";

export interface ConvexStoreOperation<
  Input extends JsonValue = JsonValue,
  Output extends JsonValue = JsonValue,
  Kind extends ConvexOperationKind = ConvexOperationKind,
> {
  readonly name: string;
  readonly kind: Kind;
  run(
    ctx: Kind extends "mutation" ? ConvexMutationContext : ConvexQueryContext,
    input: Input,
  ): Promise<Output>;
}

export interface ConvexOperationDriver {
  call<Input extends JsonValue, Output extends JsonValue>(
    operation: string,
    input: Input,
  ): Promise<Output>;
}

export class MissingConvexOperationError extends Error {
  constructor(operation: string) {
    super(`Unknown Convex store operation: ${operation}`);
    this.name = "MissingConvexOperationError";
  }
}

export class ConvexOperationKindError extends Error {
  constructor(operation: string, expected: ConvexOperationKind, actual: ConvexOperationKind) {
    super(`Convex store operation ${operation} is ${actual}; expected ${expected}`);
    this.name = "ConvexOperationKindError";
  }
}

export class InMemoryConvexOperationHost {
  private readonly operations = new Map<string, ConvexStoreOperation>();

  constructor(operations: readonly ConvexStoreOperation[] = []) {
    for (const operation of operations) {
      this.register(operation);
    }
  }

  register(operation: ConvexStoreOperation): void {
    if (this.operations.has(operation.name)) {
      throw new Error(`Duplicate Convex store operation: ${operation.name}`);
    }
    this.operations.set(operation.name, operation);
  }

  createDriver(): ConvexOperationDriver {
    return {
      call: async <Input extends JsonValue, Output extends JsonValue>(
        operation: string,
        input: Input,
      ): Promise<Output> => this.call(operation, input),
    };
  }

  async call<Input extends JsonValue, Output extends JsonValue>(
    operation: string,
    input: Input,
  ): Promise<Output> {
    const handler = this.operations.get(operation);
    if (handler === undefined) {
      throw new MissingConvexOperationError(operation);
    }
    const ctx =
      handler.kind === "mutation"
        ? ({ kind: "mutation" } satisfies ConvexMutationContext)
        : ({ kind: "query" } satisfies ConvexQueryContext);
    return (await handler.run(ctx as never, input)) as Output;
  }

  async callMutation<Input extends JsonValue, Output extends JsonValue>(
    operation: string,
    input: Input,
  ): Promise<Output> {
    return this.callWithKind(operation, input, "mutation");
  }

  async callQuery<Input extends JsonValue, Output extends JsonValue>(
    operation: string,
    input: Input,
  ): Promise<Output> {
    return this.callWithKind(operation, input, "query");
  }

  private async callWithKind<Input extends JsonValue, Output extends JsonValue>(
    operation: string,
    input: Input,
    expected: ConvexOperationKind,
  ): Promise<Output> {
    const handler = this.operations.get(operation);
    if (handler === undefined) {
      throw new MissingConvexOperationError(operation);
    }
    if (handler.kind !== expected) {
      throw new ConvexOperationKindError(operation, expected, handler.kind);
    }
    return this.call(operation, input);
  }
}
