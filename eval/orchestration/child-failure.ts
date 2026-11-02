/** Forced child failures with a real Director; all spend limits live in this eval. */
import { createAssistantMessageEventStream, type AssistantMessage } from '@earendil-works/pi-ai';
import type { StreamFn } from '@earendil-works/pi-agent-core';
import { createCallLlmStreamFn } from '@/lib/agent/runtime/stream-fn';
import type { PiStreamFnFactory } from '@/lib/chat/pi/types';
import type { ChildRuntimeMode } from '@/lib/chat/pi/child-runtime';
import type { StatelessChatRequest } from '@/lib/types/chat';
import { runPiClassroomRequest, type EvalModel } from './pi-harness';

export type ChildFailureScenario =
  | 'error'
  | 'timeout'
  | 'budget'
  | 'empty'
  | 'teacher-error'
  | 'partial';

function injectedStream(scenario: ChildFailureScenario, transport: number): ReturnType<StreamFn> {
  const stream = createAssistantMessageEventStream();
  // Native's real deadline settles this stream, without waiting 60 seconds.
  if (scenario === 'timeout') return stream;
  const partial = scenario === 'partial';
  const error = scenario === 'error' || scenario === 'teacher-error';
  const message: AssistantMessage = {
    role: 'assistant',
    api: 'eval',
    provider: 'eval',
    model: 'forced-child',
    timestamp: Date.now(),
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    content: partial
      ? [{ type: 'text', text: 'Here is the first part of the explanation.' }]
      : // An unknown tool has no side effects, and still requires continuation.
        scenario === 'budget'
        ? [
            {
              type: 'toolCall',
              id: `budget-${transport}`,
              name: 'eval_unavailable_tool',
              arguments: {},
            },
          ]
        : [],
    stopReason: error ? 'error' : partial ? 'length' : scenario === 'budget' ? 'toolUse' : 'stop',
    ...(error ? { errorMessage: 'Injected provider failure (private diagnostic)' } : {}),
  };
  queueMicrotask(() => {
    stream.push({ type: 'start', partial: message });
    if (partial) {
      stream.push({ type: 'text_start', contentIndex: 0, partial: message });
      const text = message.content[0];
      if (text.type === 'text')
        stream.push({ type: 'text_delta', contentIndex: 0, delta: text.text, partial: message });
    }
    if (error) stream.push({ type: 'error', reason: 'error', error: message });
    else
      stream.push({
        type: 'done',
        reason: message.stopReason as 'stop' | 'length' | 'toolUse',
        message,
      });
  });
  return stream;
}

export async function runChildFailureEval(
  request: StatelessChatRequest,
  model: EvalModel,
  scenario: ChildFailureScenario,
  mode: ChildRuntimeMode,
  limits = { providerCalls: 30, childCalls: 10, timeoutMs: 120_000 },
) {
  if (mode === 'legacy' && (scenario === 'timeout' || scenario === 'budget')) {
    throw new Error('Native timeout and transport-budget scenarios require native mode.');
  }
  for (const value of Object.values(limits)) {
    if (!Number.isInteger(value) || value <= 0)
      throw new Error('Eval limits must be positive integers');
  }
  const controller = new AbortController();
  const calls: string[] = [];
  const childResults: Array<{ toolCallId: string; isError: boolean; text: string }> = [];
  let providerCalls = 0;
  let injectedTransports = 0;
  let capped = false;
  let error: string | undefined;
  const cap = () => {
    capped = true;
    controller.abort();
  };
  const timer = setTimeout(cap, limits.timeoutMs);
  const factory: PiStreamFnFactory = (options, agent) => {
    const realStream = createCallLlmStreamFn(options);
    if (agent) {
      // Count delegations, not transports: a Native child may make five transports.
      if (calls.length >= limits.childCalls) cap();
      else calls.push(agent.id);
    }
    return ((...args: Parameters<StreamFn>) => {
      controller.signal.throwIfAborted();
      if (agent && (scenario !== 'teacher-error' || agent.role === 'teacher')) {
        injectedTransports += 1;
        return injectedStream(scenario, injectedTransports);
      }
      if (!agent) {
        // Capture model-visible results even when a later eval cap prevents done.
        for (const message of args[1].messages) {
          if (
            message.role === 'toolResult' &&
            message.toolName === 'call_agent' &&
            !childResults.some((result) => result.toolCallId === message.toolCallId)
          ) {
            childResults.push({
              toolCallId: message.toolCallId,
              isError: message.isError,
              text: message.content
                .map((part) => (part.type === 'text' ? part.text : '[image]'))
                .join('\n'),
            });
          }
        }
      }
      if (providerCalls >= limits.providerCalls) {
        cap();
        controller.signal.throwIfAborted();
      }
      providerCalls += 1;
      return realStream(...args);
    }) as StreamFn;
  };
  let run: Awaited<ReturnType<typeof runPiClassroomRequest>> | undefined;
  try {
    run = await runPiClassroomRequest(request, model, controller.signal, {
      childRuntimeMode: mode,
      streamFnFactory: factory,
      nativeChildTimeoutMs: scenario === 'timeout' ? 25 : undefined,
    });
  } catch (cause) {
    error = cause instanceof Error ? cause.message : String(cause);
  } finally {
    clearTimeout(timer);
  }
  const perAgent = Object.fromEntries(
    [...new Set(calls)].map((id) => [id, calls.filter((call) => call === id).length]),
  );
  return {
    scenario,
    mode,
    providerCalls,
    injectedTransports,
    childCalls: calls.length,
    perAgent,
    repeatedCalls: calls.length - Object.keys(perAgent).length,
    ending: !run
      ? 'no_done'
      : run.done.sessionClosed
        ? 'close'
        : run.done.cueUserReceived
          ? 'cue'
          : 'plain_finish',
    capped,
    error,
    childResults,
    done: run?.done,
  };
}
