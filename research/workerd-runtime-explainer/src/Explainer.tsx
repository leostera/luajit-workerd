import React from 'react';
import {
  AbsoluteFill,
  Sequence,
  interpolate,
  useCurrentFrame,
} from 'remotion';

export const FPS = 30;
export const SCENE_FRAMES = 720;
export const SCENE_COUNT = 8;

const C = {
  bg: '#000000',
  panel: '#000000',
  line: '#4a5053',
  text: '#f4f1e9',
  muted: '#a7b0ba',
  blue: '#75c9ff',
  amber: '#ffc477',
  green: '#91e0b4',
  red: '#f19588',
  dim: '#65707c',
};

const sans = 'Avenir Next, Avenir, Helvetica Neue, Arial, sans-serif';
const serif = 'Georgia, Times New Roman, serif';
const mono = 'SFMono-Regular, Menlo, Consolas, monospace';

const fade = (frame: number, from = 0, to = 24) =>
  interpolate(frame, [from, to], [0, 1], {extrapolateRight: 'clamp'});

const Box: React.FC<{
  x: number;
  y: number;
  w: number;
  h: number;
  title: string;
  detail?: string;
  color?: string;
  fill?: string;
  titleSize?: number;
}> = ({x, y, w, h, title, detail, color = C.line, fill = C.panel, titleSize = 25}) => (
  <g>
    <rect x={x} y={y} width={w} height={h} rx={18} fill={fill} stroke={color} strokeWidth={1.6} />
    <text x={x + 24} y={y + (detail ? 49 : h / 2 + 9)} fill={C.text} fontFamily={sans} fontSize={titleSize} fontWeight={600}>
      {title}
    </text>
    {detail && (
      <text x={x + 24} y={y + 82} fill={C.muted} fontFamily={mono} fontSize={17}>
        {detail}
      </text>
    )}
  </g>
);

const Arrow: React.FC<{
  x1: number;
  y1: number;
  x2: number;
  y2: number;
  color?: string;
  opacity?: number;
}> = ({x1, y1, x2, y2, color = C.line, opacity = 1}) => (
  <g opacity={opacity} stroke={color} strokeWidth={3} fill="none" strokeLinecap="round">
    <path d={`M ${x1} ${y1} L ${x2 - 12} ${y2}`} />
    <path d={`M ${x2 - 27} ${y2 - 10} L ${x2 - 12} ${y2} L ${x2 - 27} ${y2 + 10}`} />
  </g>
);

const Token: React.FC<{x: number; y: number; label: string; color?: string; opacity?: number}> = ({x, y, label, color = C.blue, opacity = 1}) => (
  <g opacity={opacity}>
    <rect x={x} y={y} width={176} height={50} rx={25} fill="#000000" stroke={color} strokeWidth={1.6} />
    <circle cx={x + 18} cy={y + 25} r={5} fill={color} />
    <text x={x + 34} y={y + 32} fill={C.text} fontFamily={mono} fontSize={18}>{label}</text>
  </g>
);

const SceneShell: React.FC<{
  index: number;
  chapter: string;
  title: string;
  caption: string;
  children: React.ReactNode;
}> = ({index, chapter, title, caption, children}) => {
  const frame = useCurrentFrame();
  const opacity = interpolate(frame, [0, 22, SCENE_FRAMES - 20, SCENE_FRAMES], [0, 1, 1, 0], {extrapolateRight: 'clamp'});
  const rise = interpolate(frame, [0, 28], [14, 0], {extrapolateRight: 'clamp'});
  return (
    <AbsoluteFill style={{backgroundColor: C.bg, color: C.text, fontFamily: sans}}>
      <div style={{position: 'absolute', top: 74, left: 150, right: 150, textAlign: 'center', color: C.muted, fontFamily: serif, fontSize: 25, fontStyle: 'italic', letterSpacing: 0.3, opacity}}>
        {chapter}
      </div>
      <div style={{position: 'absolute', top: 125, left: 150, right: 150, textAlign: 'center', opacity, transform: `translateY(${rise}px)`}}>
        <div style={{fontFamily: serif, fontStyle: 'italic', fontSize: 50, lineHeight: 1.1, letterSpacing: -0.7, maxWidth: 1550, margin: '0 auto'}}>{title}</div>
      </div>
      <div style={{position: 'absolute', left: 130, right: 130, top: 275, height: 590, opacity}}>{children}</div>
      <div style={{position: 'absolute', left: 150, right: 150, bottom: 42, textAlign: 'center', color: C.muted, fontSize: 20, letterSpacing: 0.1, opacity}}>{caption}</div>
      {index === 0 && <div style={{position: 'absolute', right: 150, bottom: 16, color: C.dim, fontFamily: mono, fontSize: 13}}>SILENT PREVIEW · NARRATION SCRIPT INCLUDED</div>}
    </AbsoluteFill>
  );
};

const Svg: React.FC<{children: React.ReactNode}> = ({children}) => (
  <svg viewBox="0 0 1600 450" width="100%" height="100%" role="img" aria-label="Runtime architecture illustration">
    <defs>
      <linearGradient id="request-line" x1="0" x2="1">
        <stop offset="0" stopColor={C.blue} stopOpacity="0.25" />
        <stop offset="1" stopColor={C.blue} />
      </linearGradient>
      <linearGradient id="return-line" x1="1" x2="0">
        <stop offset="0" stopColor={C.green} stopOpacity="0.25" />
        <stop offset="1" stopColor={C.green} />
      </linearGradient>
    </defs>
    {children}
  </svg>
);

const RequestIntro: React.FC = () => {
  const frame = useCurrentFrame();
  const x = interpolate(frame, [12, 125], [120, 960], {extrapolateLeft: 'clamp', extrapolateRight: 'clamp'});
  const line = interpolate(frame, [20, 130], [0, 1], {extrapolateRight: 'clamp'});
  return (
    <Svg>
      <text x={120} y={115} fill={C.muted} fontFamily={mono} fontSize={18}>CLIENT</text>
      <path d={`M 296 185 H ${320 + line * 620}`} stroke={C.blue} strokeWidth={3} strokeOpacity={0.55} fill="none" />
      <rect x={780} y={76} width={570} height={280} rx={30} fill="#000000" stroke="#52606d" strokeWidth={2} />
      <text x={820} y={122} fill={C.muted} fontFamily={mono} fontSize={17}>WORKERD SERVER</text>
      <path d="M 930 190 H 1120 M 1120 190 V 155 M 1120 190 V 265 M 1120 155 H 1190 M 1120 265 H 1190" stroke={C.line} strokeWidth={2} fill="none" />
      <Box x={1190} y={118} w={235} h={74} title="V8" color={C.blue} titleSize={26} />
      <Box x={1190} y={228} w={235} h={74} title="LuaJIT" color={C.amber} titleSize={26} />
      <Token x={x} y={160} label="GET /hello" opacity={frame < 12 ? 0 : 1} />
      <text x={780} y={400} fill={C.muted} fontFamily={serif} fontSize={25} fontStyle="italic">One request path. An explicit runtime choice.</text>
    </Svg>
  );
};

const ServerPath: React.FC = () => {
  const frame = useCurrentFrame();
  const x = interpolate(frame, [18, 255], [80, 1340], {extrapolateLeft: 'clamp', extrapolateRight: 'clamp'});
  return (
    <Svg>
      <Arrow x1={300} y1={205} x2={430} y2={205} color={C.blue} />
      <Arrow x1={690} y1={205} x2={820} y2={205} color={C.blue} />
      <Arrow x1={1080} y1={205} x2={1220} y2={205} color={C.blue} />
      <Box x={60} y={148} w={245} h={116} title="Client" detail="GET /hello" color={C.blue} />
      <Box x={430} y={148} w={260} h={116} title="Listener" detail="configured socket" />
      <Box x={820} y={148} w={260} h={116} title="Named service" detail="config route" />
      <Box x={1220} y={148} w={300} h={116} title="Worker handler" detail="fetch(request)" color={C.amber} />
      <path d="M 80 55 H 1520" stroke={C.line} strokeWidth={2} strokeDasharray="5 12" opacity={0.65} />
      <Token x={x} y={30} label="GET /hello" />
      <text x={430} y={340} fill={C.dim} fontFamily={mono} fontSize={17}>SOCKET</text>
      <text x={820} y={340} fill={C.dim} fontFamily={mono} fontSize={17}>SERVICE GRAPH</text>
      <text x={1220} y={340} fill={C.dim} fontFamily={mono} fontSize={17}>RUNTIME-SPECIFIC</text>
      <text x={62} y={405} fill={C.muted} fontFamily={serif} fontSize={25} fontStyle="italic">Config routes traffic; the runtime handles code.</text>
    </Svg>
  );
};

const V8Path: React.FC = () => {
  const frame = useCurrentFrame();
  const dot = interpolate(frame, [18, 250], [40, 1270], {extrapolateLeft: 'clamp', extrapolateRight: 'clamp'});
  return (
    <Svg>
      <Arrow x1={250} y1={185} x2={385} y2={185} color={C.blue} />
      <Arrow x1={660} y1={185} x2={790} y2={185} color={C.blue} />
      <Arrow x1={1060} y1={185} x2={1190} y2={185} color={C.blue} />
      <path d="M 40 120 H 1500" stroke={C.line} strokeWidth={2} strokeDasharray="5 12" opacity={0.65} />
      <Box x={40} y={132} w={210} h={106} title="HTTP" detail="GET /hello" color={C.blue} />
      <Box x={385} y={132} w={275} h={106} title="Server service" detail="request dispatch" />
      <Box x={790} y={132} w={270} h={106} title="Worker / Script" detail="existing JS path" color={C.blue} />
      <Box x={1190} y={132} w={300} h={106} title="JSG + V8" detail="Glue layer → JS engine" color={C.blue} fill="#000000" />
      <path d="M 790 292 H 1490" stroke={C.line} strokeWidth={2} strokeDasharray="7 12" />
      <text x={790} y={335} fill={C.muted} fontFamily={mono} fontSize={19}>V8 IS PART OF A DEEPER WORKER + API STACK</text>
      <Token x={dot} y={54} label="GET /hello" opacity={frame > 250 ? 0 : 0.95} />
      <text x={385} y={390} fill={C.dim} fontFamily={serif} fontSize={24} fontStyle="italic">Lua does not emulate this JavaScript contract.</text>
    </Svg>
  );
};

const SeamDiagram: React.FC = () => {
  const frame = useCurrentFrame();
  const glow = fade(frame, 40, 72);
  return (
    <Svg>
      <Box x={60} y={125} w={300} h={125} title="Server service" detail="starts one request" />
      <Arrow x1={360} y1={187} x2={530} y2={187} color={C.blue} />
      <Box x={530} y={112} w={430} h={150} title="WorkerInterface" detail="HTTP request → promise" color={C.blue} fill="#000000" />
      <Arrow x1={960} y1={187} x2={1115} y2={187} color={C.blue} />
      <Box x={1115} y={65} w={350} h={90} title="V8 Worker" detail="existing implementation" color={C.blue} />
      <Box x={1115} y={220} w={350} h={90} title="Lua adapter" detail="new implementation" color={C.amber} />
      <path d="M 1085 110 V 265" stroke={C.line} strokeWidth={2} strokeDasharray="6 9" />
      <text x={1115} y={29} fill={C.dim} fontFamily={mono} fontSize={15}>ONE IMPLEMENTATION PER CONFIGURED WORKER</text>
      <text x={60} y={342} fill={C.muted} fontFamily={serif} fontSize={25} fontStyle="italic" opacity={glow}>A host request seam—not a language compatibility layer.</text>
      <text x={1115} y={355} fill={C.red} fontFamily={mono} fontSize={17}>OTHER EVENT TYPES: NOT IMPLEMENTED</text>
    </Svg>
  );
};

const RuntimeChoice: React.FC = () => {
  const frame = useCurrentFrame();
  const pulse = interpolate(frame, [30, 65, 100], [0.35, 1, 0.35], {extrapolateRight: 'clamp'});
  return (
    <Svg>
      <rect x={50} y={40} width={625} height={340} rx={18} fill="#000000" stroke="#36404a" strokeWidth={2} />
      <text x={90} y={85} fill={C.muted} fontFamily={mono} fontSize={17}>WORKER CONFIG</text>
      <text x={90} y={145} fill={C.text} fontFamily={mono} fontSize={22}>worker = (</text>
      <text x={125} y={190} fill={C.amber} fontFamily={mono} fontSize={22}>luaScript = embed "worker.lua"</text>
      <text x={90} y={235} fill={C.text} fontFamily={mono} fontSize={22}>)</text>
      <text x={90} y={320} fill={C.dim} fontFamily={mono} fontSize={16}>-- select at service construction</text>
      <Arrow x1={675} y1={210} x2={835} y2={210} color={C.amber} opacity={pulse} />
      <Box x={835} y={140} w={300} h={135} title="LuaWorkerService" detail="one LuaJIT state" color={C.amber} fill="#000000" />
      <Arrow x1={1135} y1={210} x2={1280} y2={210} color={C.amber} opacity={pulse} />
      <Box x={1280} y={140} w={265} h={135} title="fetch(request)" detail="HTTP-only handler" color={C.amber} />
      <path d="M 835 330 H 1545" stroke={C.blue} strokeWidth={2} strokeDasharray="8 11" opacity={0.7} />
      <text x={835} y={366} fill={C.blue} fontFamily={mono} fontSize={17}>V8 WORKERS KEEP THEIR EXISTING PATH</text>
    </Svg>
  );
};

const HttpContract: React.FC = () => {
  const frame = useCurrentFrame();
  const x = interpolate(frame, [22, 160], [0, 1], {extrapolateRight: 'clamp'});
  return (
    <Svg>
      <Box x={35} y={130} w={310} h={140} title="HTTP request" detail="method · URL · headers · body" color={C.blue} />
      <Arrow x1={345} y1={200} x2={490} y2={200} color={C.blue} opacity={x} />
      <Box x={490} y={108} w={445} h={184} title="Lua request table" detail="request.method / request.url" color={C.amber} fill="#000000" />
      <text x={535} y={255} fill={C.muted} fontFamily={mono} fontSize={16}>headers[] · body</text>
      <Arrow x1={935} y1={200} x2={1080} y2={200} color={C.green} opacity={x} />
      <Box x={1080} y={108} w={445} h={184} title="Lua response table" detail="status · headers · body" color={C.green} fill="#000000" />
      <path d="M 1510 325 H 120 M 120 325 V 304" stroke={C.green} strokeWidth={3} fill="none" opacity={0.65} />
      <path d="M 120 304 L 108 322 L 132 322 Z" fill={C.green} opacity={0.8} />
      <text x={540} y={374} fill={C.muted} fontFamily={serif} fontSize={24} fontStyle="italic">A small, explicit contract. No env. No ctx.</text>
    </Svg>
  );
};

const Coroutine: React.FC = () => {
  const frame = useCurrentFrame();
  const tokenX = interpolate(frame, [20, 140], [120, 630], {extrapolateRight: 'clamp'});
  const tokenOpacity = interpolate(frame, [0, 18, 132, 154], [0, 1, 1, 0], {extrapolateRight: 'clamp'});
  const timerProgress = interpolate(frame, [145, 350], [0, 1], {extrapolateLeft: 'clamp', extrapolateRight: 'clamp'});
  const otherX = interpolate(frame, [185, 330], [650, 1300], {extrapolateLeft: 'clamp', extrapolateRight: 'clamp'});
  const resumeX = interpolate(frame, [350, 470], [815, 1330], {extrapolateLeft: 'clamp', extrapolateRight: 'clamp'});
  return (
    <Svg>
      <text x={75} y={74} fill={C.muted} fontFamily={mono} fontSize={17}>REQUEST A</text>
      <path d="M 120 155 H 1440" stroke={C.line} strokeWidth={2} />
      <Token x={tokenX} y={130} label="Lua coroutine" color={C.blue} opacity={tokenOpacity} />
      <rect x={640} y={106} width={175} height={98} rx={16} fill="#000000" stroke={C.amber} strokeWidth={2} opacity={frame > 130 && frame < 360 ? 1 : 0.18} />
      <text x={662} y={145} fill={C.amber} fontFamily={mono} fontSize={17} opacity={frame > 130 && frame < 360 ? 1 : 0.3}>sleep(50)</text>
      <text x={663} y={177} fill={C.muted} fontFamily={mono} fontSize={14} opacity={frame > 130 && frame < 360 ? 1 : 0.3}>yield</text>
      <text x={855} y={125} fill={C.amber} fontFamily={mono} fontSize={17} opacity={frame > 130 && frame < 360 ? 1 : 0.3}>KJ timer promise</text>
      <rect x={855} y={147} width={350} height={7} rx={4} fill="#343a40" />
      <rect x={855} y={147} width={350 * timerProgress} height={7} rx={4} fill={C.amber} />
      <text x={75} y={272} fill={C.muted} fontFamily={mono} fontSize={17}>WHILE A WAITS</text>
      <path d="M 120 338 H 1440" stroke={C.line} strokeWidth={2} />
      <Token x={otherX} y={313} label="request B" color={C.blue} opacity={frame > 170 ? 1 : 0.15} />
      <text x={120} y={405} fill={C.green} fontFamily={serif} fontSize={24} fontStyle="italic" opacity={frame > 355 ? 1 : 0.3}>Timer resolves → coroutine resumes → response</text>
      <Token x={resumeX} y={130} label="resumed" color={C.green} opacity={frame > 350 ? 1 : 0} />
    </Svg>
  );
};

const Boundaries: React.FC = () => {
  const frame = useCurrentFrame();
  const opacity = fade(frame, 40, 85);
  return (
    <Svg>
      <Box x={65} y={110} w={400} h={185} title="workerd server" detail="config · service · HTTP" color={C.blue} />
      <Arrow x1={465} y1={202} x2={625} y2={202} color={C.blue} />
      <rect x={625} y={62} width={475} height={284} rx={28} fill="#000000" stroke={C.amber} strokeWidth={2} opacity={opacity} />
      <text x={665} y={112} fill={C.amber} fontFamily={mono} fontSize={18}>EXPERIMENTAL · TRUSTED CODE</text>
      <text x={665} y={173} fill={C.text} fontFamily={sans} fontSize={26} fontWeight={600}>LuaJIT runtime</text>
      <text x={665} y={218} fill={C.muted} fontFamily={mono} fontSize={17}>HTTP only · no bindings</text>
      <text x={665} y={258} fill={C.red} fontFamily={mono} fontSize={16}>no sandbox · no CPU / memory limits</text>
      <Arrow x1={1100} y1={202} x2={1250} y2={202} color={C.green} />
      <Box x={1250} y={110} w={285} h={185} title="HTTP response" detail="status + body" color={C.green} />
      <text x={65} y={386} fill={C.muted} fontFamily={serif} fontSize={25} fontStyle="italic">A new runtime path—not a new security boundary.</text>
    </Svg>
  );
};

const scenes = [
  {chapter: 'The request', title: 'One request. Two runtimes.', caption: 'How does workerd route a request—and where can LuaJIT fit?', Visual: RequestIntro},
  {chapter: 'The server', title: 'Workerd is more than a language VM.', caption: 'Config routes traffic; the runtime handles code.', Visual: ServerPath},
  {chapter: 'The existing path', title: 'JavaScript runs through JSG and V8.', caption: 'The Lua backend leaves this established path intact.', Visual: V8Path},
  {chapter: 'The extension seam', title: 'Attach at the host request boundary.', caption: 'WorkerInterface carries the supported HTTP request and response.', Visual: SeamDiagram},
  {chapter: 'Runtime selection', title: 'Choose LuaJIT in worker config.', caption: 'One Lua state per configured service; a request adapter per call.', Visual: RuntimeChoice},
  {chapter: 'The contract', title: 'Translate HTTP into a small Lua API.', caption: 'fetch(request) in. A status, headers, and body out.', Visual: HttpContract},
  {chapter: 'Coroutines', title: 'Yield while the timer waits.', caption: 'A suspended handler leaves the event loop free for another request.', Visual: Coroutine},
  {chapter: 'The boundary', title: 'Additive, experimental, and deliberately limited.', caption: 'LuaJIT is not a sandbox. Synchronous Lua can still block.', Visual: Boundaries},
] as const;

const Scene: React.FC<{index: number; chapter: string; title: string; caption: string; Visual: React.FC}> = ({index, chapter, title, caption, Visual}) => (
  <SceneShell index={index} chapter={chapter} title={title} caption={caption}>
    <Visual />
  </SceneShell>
);

export const Explainer: React.FC = () => (
  <AbsoluteFill style={{backgroundColor: C.bg}}>
    {scenes.map((scene, index) => (
      <Sequence key={scene.chapter} from={index * SCENE_FRAMES} durationInFrames={SCENE_FRAMES}>
        <Scene index={index} {...scene} />
      </Sequence>
    ))}
  </AbsoluteFill>
);
