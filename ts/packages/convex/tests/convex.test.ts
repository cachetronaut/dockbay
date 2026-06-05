import { describe, expect, it } from "vitest";
import {
  ConvexOperationKindError,
  type ConvexStoreOperation,
  InMemoryConvexOperationHost,
  MissingConvexOperationError,
} from "../src/index.js";

describe("Convex operation driver", () => {
  it("dispatches named mutation operations through a test host", async () => {
    const reserve: ConvexStoreOperation<
      { readonly amount: number },
      { readonly ok: boolean; readonly reserved: number },
      "mutation"
    > = {
      name: "budget.tryReserve",
      kind: "mutation",
      async run(ctx, input) {
        expect(ctx.kind).toBe("mutation");
        return { ok: true, reserved: input.amount };
      },
    };
    const driver = new InMemoryConvexOperationHost([reserve]).createDriver();

    await expect(driver.call("budget.tryReserve", { amount: 2 })).resolves.toEqual({
      ok: true,
      reserved: 2,
    });
  });

  it("dispatches named query operations separately from mutations", async () => {
    const isRevoked: ConvexStoreOperation<
      { readonly jti: string },
      { readonly revoked: boolean },
      "query"
    > = {
      name: "revocation.isRevoked",
      kind: "query",
      async run(ctx, input) {
        expect(ctx.kind).toBe("query");
        return { revoked: input.jti === "jti_1" };
      },
    };
    const host = new InMemoryConvexOperationHost([isRevoked]);

    await expect(host.callQuery("revocation.isRevoked", { jti: "jti_1" })).resolves.toEqual({
      revoked: true,
    });
    await expect(
      host.callMutation("revocation.isRevoked", { jti: "jti_1" }),
    ).rejects.toBeInstanceOf(ConvexOperationKindError);
  });

  it("fails clearly when an operation is not deployed", async () => {
    const driver = new InMemoryConvexOperationHost().createDriver();

    await expect(driver.call("budget.tryReserve", { amount: 1 })).rejects.toBeInstanceOf(
      MissingConvexOperationError,
    );
  });
});
