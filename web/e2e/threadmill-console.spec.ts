import { expect, test, type Page } from "@playwright/test";

const projectURL =
  "/?project_id=demo-project&conversation_id=demo-project-manager";

async function openConsole(page: Page) {
  await page.goto(projectURL);
  await expect(
    page.getByRole("heading", { name: "Coordination Graph" }),
  ).toBeVisible();
  await expect(page.locator(".connection-status")).toContainText("Live");
  await page.getByRole("button", { name: "List", exact: true }).click();
}

function graphRevision(page: Page) {
  return page
    .locator(".command-meta .revision-label")
    .filter({ hasText: "Graph revision" })
    .locator("strong");
}

function desiredConcurrency(page: Page) {
  return page
    .locator(".capacity-values > div")
    .filter({ hasText: "Desired" })
    .locator("dd");
}

function taskAlphaExecute(page: Page) {
  return page
    .locator("section.task-group")
    .filter({ hasText: "task-alpha" })
    .getByRole("button", { name: /execute/ });
}

test.describe.serial("Threadmill operator console", () => {
  test("adjusts Agent capacity without mutating the Coordination Graph", async ({
    page,
  }) => {
    const graphWrites: string[] = [];
    page.on("request", (request) => {
      const path = new URL(request.url()).pathname;
      if (request.method() !== "GET" && path.startsWith("/v1/coordination/")) {
        graphWrites.push(`${request.method()} ${path}`);
      }
    });

    await openConsole(page);
    const revisionBefore = await graphRevision(page).textContent();
    const desiredBefore = Number(await desiredConcurrency(page).textContent());

    await page.locator(".capacity-actions .primary-icon-button").click();
    await expect(desiredConcurrency(page)).toHaveText(
      String(desiredBefore + 1),
    );
    await expect(graphRevision(page)).toHaveText(revisionBefore ?? "");

    await page.locator(".capacity-actions button").first().click();
    await expect(desiredConcurrency(page)).toHaveText(String(desiredBefore));
    await expect(graphRevision(page)).toHaveText(revisionBefore ?? "");
    expect(graphWrites).toEqual([]);
  });

  test("shows subscriptions, materialized Context, and created candidates separately", async ({
    page,
  }) => {
    await openConsole(page);
    await taskAlphaExecute(page).click();

    await expect(
      page.getByRole("heading", { name: "Subscription subgraphs" }),
    ).toBeVisible();
    await expect(page.locator(".subscription-list > li")).toHaveCount(2);
    await expect(
      page.getByRole("heading", { name: "Context Slice" }),
    ).toBeVisible();
    await expect(page.locator(".context-node-list")).toContainText(
      "demo context for task-alpha",
    );
    await expect(
      page.getByRole("heading", { name: "TaskMemoryBuffer" }),
    ).toBeVisible();
    await expect(page.locator(".candidate-list")).toContainText(
      "candidate from invocation://task-alpha/execute/1/1",
    );
    await expect(page.locator(".inspector-panel")).not.toContainText(
      "must not leak",
    );
    await expect(
      page.locator(".react-flow__node").filter({ hasText: "candidate from" }),
    ).toHaveCount(0);
  });

  test("routes hold and resume through Manager and keeps stop distinct from hold", async ({
    page,
  }) => {
    const graphWrites: string[] = [];
    page.on("request", (request) => {
      const path = new URL(request.url()).pathname;
      if (request.method() !== "GET" && path.startsWith("/v1/coordination/")) {
        graphWrites.push(`${request.method()} ${path}`);
      }
    });

    await openConsole(page);
    const execute = taskAlphaExecute(page);
    await execute.click();
    await expect(page.locator(".inspector-panel")).toBeVisible();
    const oldInvocation = await page
      .locator(".inspector-meta .wide dd")
      .textContent();

    await page.getByRole("tab", { name: "Manager", exact: true }).click();
    await page.locator("#manager-message").fill("hold current execute");
    await page.getByRole("button", { name: "Send" }).click();
    await expect(execute).toContainText("held");
    await expect(page.locator(".conversation")).toContainText(
      "Input manager-input://",
    );
    await expect(page.locator(".conversation")).toContainText(
      "Decision decision://",
    );

    await page.getByRole("tab", { name: "Phase inspector" }).click();
    await expect(
      page.locator(".inspector-panel .status-label").first(),
    ).toHaveText("stopped");

    await page.getByRole("tab", { name: "Manager", exact: true }).click();
    await page.locator("#manager-message").fill("resume current execute");
    await page.getByRole("button", { name: "Send" }).click();
    await expect(execute).toContainText("gen 2");
    await expect(execute).not.toContainText("held");

    await execute.click();
    await expect(page.locator(".inspector-meta")).toContainText("2");
    const newInvocation = await page
      .locator(".inspector-meta .wide dd")
      .textContent();
    expect(newInvocation).toBeTruthy();
    expect(newInvocation).not.toBe(oldInvocation);
    await expect(
      page.locator(".inspector-panel .status-label").first(),
    ).toHaveText("running");
    expect(graphWrites).toEqual([]);
  });

  test("reconnects the projection after a finite SSE response", async ({
    page,
  }) => {
    let streams = 0;
    await page.route("**/v1/events/stream?**", async (route) => {
      streams++;
      if (streams === 1) {
        const occurredAt = new Date().toISOString();
        const event = {
          event_id: "evt-e2e-reconnect",
          cursor: "999",
          type: "manager.interaction",
          occurred_at: occurredAt,
          project_id: "demo-project",
          payload: {
            kind: "decision",
            created_at: occurredAt,
            manager_input_ref: "manager-input://e2e-reconnect",
            decision_ref: "decision://e2e-reconnect",
            graph_revision: 1,
            disposition: "accepted",
          },
        };
        await route.fulfill({
          status: 200,
          headers: {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
          },
          body: `id: 999\nevent: manager.interaction\ndata: ${JSON.stringify(event)}\n\n`,
        });
        return;
      }
      await route.continue();
    });

    await page.goto(projectURL);
    await expect(
      page.getByRole("heading", { name: "Coordination Graph" }),
    ).toBeVisible();
    await expect(page.locator(".connection-status")).toContainText(
      "Reconnecting",
    );
    await expect(page.locator(".connection-status")).toContainText("Live", {
      timeout: 15_000,
    });
    expect(streams).toBeGreaterThanOrEqual(2);
    await page.unrouteAll({ behavior: "ignoreErrors" });
    await page.close();
  });
});
