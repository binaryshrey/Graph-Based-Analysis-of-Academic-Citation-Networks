import type { DesignSystem, Page, SlideMeta } from '@open-slide/core';

import nyuLogo from './assets/nyu.svg';

// ─── Panel-tweakable design tokens ────────────────────────────────────────────
// Edit live from the Design panel; consumed via `var(--osd-*)` in inline styles.
export const design: DesignSystem = {
  palette: {
    bg: '#08090a',
    text: '#f7f8f8',
    accent: '#7170ff',
  },
  fonts: {
    display: '"Inter", "SF Pro Display", system-ui, -apple-system, sans-serif',
    body: '"Inter", "SF Pro Display", system-ui, -apple-system, sans-serif',
  },
  typeScale: {
    hero: 168,
    body: 36,
  },
  radius: 16,
};

// ─── Local (non-tweakable) constants ──────────────────────────────────────────
const palette = {
  bg: design.palette.bg,
  text: design.palette.text,
  accent: design.palette.accent,
  surface: '#0e0f12',
  surfaceHi: '#14161a',
  surfaceMax: '#1a1c21',
  textSoft: '#c7c9d1',
  muted: '#6f727c',
  dim: '#3e4048',
  border: 'rgba(255,255,255,0.07)',
  borderBright: 'rgba(255,255,255,0.14)',
  accentSoft: '#a3a0ff',
  accent2: '#5e6ad2',
  mint: '#68cc9a',
  amber: '#e0b25c',
  cyan: '#22d3ee',
  pink: '#ec4899',
};

const font = {
  sans: design.fonts.body,
  display: design.fonts.display,
  mono: '"JetBrains Mono", "SF Mono", ui-monospace, Menlo, monospace',
};

const fill = {
  width: '100%',
  height: '100%',
  background: 'var(--osd-bg)',
  color: 'var(--osd-text)',
  fontFamily: 'var(--osd-font-body)',
  letterSpacing: '-0.015em',
  overflow: 'hidden',
  position: 'relative' as const,
};

// ─── Shared animations ───────────────────────────────────────────────────────
const styles = `
  @keyframes cn-fadeUp {
    from { opacity: 0; transform: translateY(18px); }
    to   { opacity: 1; transform: translateY(0); }
  }
  @keyframes cn-fadeIn {
    from { opacity: 0; }
    to   { opacity: 1; }
  }
  @keyframes cn-scaleIn {
    from { opacity: 0; transform: scale(0.92); }
    to   { opacity: 1; transform: scale(1); }
  }
  @keyframes cn-slideRight {
    from { opacity: 0; transform: translateX(-24px); }
    to   { opacity: 1; transform: translateX(0); }
  }
  @keyframes cn-pulse {
    0%, 100% { opacity: 0.4; }
    50%      { opacity: 0.8; }
  }
  @keyframes cn-drawLine {
    from { stroke-dashoffset: 1000; }
    to   { stroke-dashoffset: 0; }
  }
  @keyframes cn-nodeAppear {
    from { opacity: 0; r: 0; }
    to   { opacity: 1; }
  }
  .cn-fadeUp  { opacity: 0; animation: cn-fadeUp 0.9s cubic-bezier(.2,.7,.2,1) forwards; }
  .cn-fadeIn  { opacity: 0; animation: cn-fadeIn 1.2s ease forwards; }
  .cn-scaleIn { opacity: 0; animation: cn-scaleIn 0.7s cubic-bezier(.2,.7,.2,1) forwards; }
  .cn-slideRight { opacity: 0; animation: cn-slideRight 0.8s cubic-bezier(.2,.7,.2,1) forwards; }
`;

const Styles = () => <style>{styles}</style>;

// ─── Shared chrome ───────────────────────────────────────────────────────────
const GridBg = () => (
  <div
    style={{
      position: 'absolute',
      inset: 0,
      backgroundImage:
        'linear-gradient(rgba(255,255,255,0.025) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.025) 1px, transparent 1px)',
      backgroundSize: '96px 96px',
      maskImage: 'radial-gradient(ellipse at center, rgba(0,0,0,0.9) 0%, rgba(0,0,0,0) 70%)',
      WebkitMaskImage: 'radial-gradient(ellipse at center, rgba(0,0,0,0.9) 0%, rgba(0,0,0,0) 70%)',
    }}
  />
);

const Eyebrow = ({
  children,
  style: extraStyle,
  className,
}: {
  children: React.ReactNode;
  style?: React.CSSProperties;
  className?: string;
}) => (
  <div
    className={className}
    style={{
      fontFamily: font.mono,
      fontSize: 22,
      letterSpacing: '0.18em',
      textTransform: 'uppercase',
      color: palette.muted,
      ...extraStyle,
    }}
  >
    {children}
  </div>
);

const PageNum = ({ n, total = 8 }: { n: number; total?: number }) => (
  <div
    style={{
      position: 'absolute',
      bottom: 44,
      right: 60,
      fontFamily: font.mono,
      fontSize: 20,
      color: palette.dim,
      letterSpacing: '0.08em',
    }}
  >
    {String(n).padStart(2, '0')} / {String(total).padStart(2, '0')}
  </div>
);

// Network-graph background SVG shared across several pages
const NyuLogo = () => (
  <img
    className="cn-fadeIn"
    src={nyuLogo}
    alt="NYU"
    style={{ position: 'absolute', top: 40, right: 60, height: 56, zIndex: 2 }}
  />
);

const NetworkBgSvg = ({ opacity = 0.08 }: { opacity?: number }) => (
  <svg
    style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', opacity }}
    viewBox="0 0 1920 1080"
    fill="none"
  >
    {[
      [200, 150, 500, 300], [500, 300, 800, 200], [800, 200, 1100, 400],
      [1100, 400, 1400, 250], [1400, 250, 1700, 350], [300, 650, 600, 780],
      [600, 780, 950, 680], [950, 680, 1300, 830], [1300, 830, 1600, 720],
      [500, 300, 600, 780], [800, 200, 950, 680], [1100, 400, 1300, 830],
    ].map(([x1, y1, x2, y2], i) => (
      <line
        key={i}
        x1={x1} y1={y1} x2={x2} y2={y2}
        stroke={i % 2 === 0 ? palette.accent : palette.accentSoft}
        strokeWidth={1}
        strokeDasharray="1000"
        strokeDashoffset="1000"
        style={{ animation: `cn-drawLine 2s ease forwards ${0.3 + i * 0.15}s` }}
      />
    ))}
    {[
      [200, 150], [500, 300], [800, 200], [1100, 400], [1400, 250], [1700, 350],
      [300, 650], [600, 780], [950, 680], [1300, 830], [1600, 720],
    ].map(([cx, cy], i) => (
      <circle
        key={i}
        cx={cx} cy={cy}
        r={3 + (i % 3) * 2}
        fill={i % 3 === 0 ? palette.accent : i % 3 === 1 ? palette.mint : palette.accentSoft}
        style={{ opacity: 0, animation: `cn-fadeIn 0.5s ease forwards ${1.2 + i * 0.12}s` }}
      />
    ))}
  </svg>
);

// ═══════════════════════════════════════════════════════════════════════════════
// PAGE 1 — Cover
// ═══════════════════════════════════════════════════════════════════════════════
const Cover: Page = () => (
  <div style={fill}>
    <Styles />
    <GridBg />
    <NetworkBgSvg opacity={0.06} />
    <NyuLogo />
    <div
      style={{
        position: 'absolute',
        inset: 0,
        padding: '120px 140px',
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'space-between',
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Eyebrow className="cn-fadeUp" style={{ animationDelay: '0.05s' }}>
          Big Data · CS-GY 6513 C · Spring 2026
        </Eyebrow>
        <div
          className="cn-fadeUp"
          style={{
            animationDelay: '0.05s',
            fontFamily: font.mono,
            fontSize: 20,
            color: palette.muted,
            border: `1px solid ${palette.border}`,
            padding: '8px 16px',
            borderRadius: 999,
          }}
        >
          New York University
        </div>
      </div>

      <div>
        <h1
          className="cn-fadeUp"
          style={{
            fontFamily: 'var(--osd-font-display)',
            fontSize: 108,
            lineHeight: 1,
            fontWeight: 600,
            margin: 0,
            letterSpacing: '-0.045em',
            animationDelay: '0.15s',
          }}
        >
          Graph-Based Analysis
          <br />
          <span style={{ fontSize: 94 }}>of{' '}
            <span
              style={{
                background: `linear-gradient(90deg, ${palette.accentSoft}, var(--osd-accent))`,
                WebkitBackgroundClip: 'text',
                backgroundClip: 'text',
                color: 'transparent',
              }}
            >
              Citation Networks
            </span>
          </span>
        </h1>
        <p
          className="cn-fadeUp"
          style={{
            marginTop: 40,
            maxWidth: 1100,
            fontSize: 'var(--osd-size-body)',
            lineHeight: 1.35,
            color: palette.textSoft,
            animationDelay: '0.35s',
          }}
        >
          Using Spark and GraphFrames to analyze millions of academic citations, discovering research communities and identifying the most influential papers.
        </p>
      </div>

      <div
        className="cn-fadeUp"
        style={{
          animationDelay: '0.55s',
          display: 'flex',
          gap: 36,
        }}
      >
        {[
          { name: 'Navya Gupta', id: 'NG3118' },
          { name: 'Hardik Amarwani', id: 'HA3290' },
          { name: 'Shreyansh Saurabh', id: 'SS21034' },
        ].map((m, i) => (
          <div
            key={i}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 14,
              padding: '12px 22px',
              background: palette.surface,
              border: `1px solid ${palette.border}`,
              borderRadius: 'var(--osd-radius)',
            }}
          >
            <div
              style={{
                width: 38,
                height: 38,
                borderRadius: '50%',
                background: `linear-gradient(135deg, ${palette.accent}, ${palette.accentSoft})`,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontSize: 17,
                fontWeight: 700,
              }}
            >
              {m.name[0]}
            </div>
            <div>
              <div style={{ fontSize: 22, fontWeight: 600 }}>{m.name}</div>
              <div style={{ fontSize: 16, fontFamily: font.mono, color: palette.muted }}>
                {m.id}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
    <PageNum n={1} />
  </div>
);

// ═══════════════════════════════════════════════════════════════════════════════
// PAGE 2 — Abstract
// ═══════════════════════════════════════════════════════════════════════════════
const Abstract: Page = () => (
  <div style={fill}>
    <Styles />
    <GridBg />
    <NyuLogo />
    <div
      style={{
        position: 'absolute',
        inset: 0,
        padding: '100px 140px',
        display: 'flex',
        flexDirection: 'column',
        gap: 44,
      }}
    >
      <div className="cn-fadeUp">
        <Eyebrow>Abstract</Eyebrow>
        <h2
          style={{
            marginTop: 20,
            marginBottom: 0,
            fontFamily: 'var(--osd-font-display)',
            fontSize: 80,
            fontWeight: 600,
            letterSpacing: '-0.035em',
            lineHeight: 1.02,
          }}
        >
          Scalable graph analytics for{' '}
          <span
            style={{
              background: `linear-gradient(90deg, ${palette.accentSoft}, var(--osd-accent))`,
              WebkitBackgroundClip: 'text',
              backgroundClip: 'text',
              color: 'transparent',
            }}
          >
            research discovery.
          </span>
        </h2>
      </div>

      <div
        style={{
          flex: 1,
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: 32,
          minHeight: 0,
        }}
      >
        {[
          {
            label: 'Framework',
            text: 'A scalable big-data framework built on Apache Spark and GraphFrames for uncovering thematic research communities in large-scale academic citation networks.',
          },
          {
            label: 'Data Source',
            text: 'Ingests NYU-affiliated scholarly metadata from OpenAlex and constructs a directed citation graph linking hundreds of thousands of academic papers.',
          },
          {
            label: 'Analytics',
            text: 'Applies Louvain Community Detection to identify research clusters and leverages weighted PageRank to rank influential publications within each community.',
          },
          {
            label: 'Visualization',
            text: 'PyVis enables interactive visual exploration with nodes color-coded by community and sized by influence — rendered as explorable HTML in the browser.',
          },
        ].map((item, i) => (
          <div
            key={i}
            className="cn-scaleIn"
            style={{
              padding: '36px 40px',
              background: palette.surface,
              border: `1px solid ${palette.border}`,
              borderRadius: 'var(--osd-radius)',
              display: 'flex',
              flexDirection: 'column',
              gap: 16,
              animationDelay: `${0.3 + i * 0.15}s`,
            }}
          >
            <div
              style={{
                fontFamily: font.mono,
                fontSize: 18,
                letterSpacing: '0.15em',
                textTransform: 'uppercase',
                color: palette.accentSoft,
              }}
            >
              {item.label}
            </div>
            <div style={{ fontSize: 26, lineHeight: 1.55, color: palette.textSoft }}>
              {item.text}
            </div>
          </div>
        ))}
      </div>
    </div>
    <PageNum n={2} />
  </div>
);

// ═══════════════════════════════════════════════════════════════════════════════
// PAGE 3 — Problem Statement & Objectives
// ═══════════════════════════════════════════════════════════════════════════════
const ProblemObjectives: Page = () => (
  <div style={fill}>
    <Styles />
    <GridBg />
    <NyuLogo />
    <div
      style={{
        position: 'absolute',
        inset: 0,
        padding: '90px 140px',
        display: 'flex',
        flexDirection: 'column',
        gap: 40,
      }}
    >
      <div className="cn-fadeUp">
        <Eyebrow>Problem Statement &amp; Objectives</Eyebrow>
        <h2
          style={{
            marginTop: 20,
            marginBottom: 0,
            fontFamily: 'var(--osd-font-display)',
            fontSize: 80,
            fontWeight: 600,
            letterSpacing: '-0.035em',
            lineHeight: 1.02,
          }}
        >
          Why graph analytics?
        </h2>
      </div>

      <div style={{ flex: 1, display: 'grid', gridTemplateColumns: '1.1fr 1fr', gap: 48, minHeight: 0 }}>
        {/* Problem */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
          <div
            style={{
              fontFamily: font.mono,
              fontSize: 18,
              letterSpacing: '0.15em',
              textTransform: 'uppercase',
              color: palette.amber,
            }}
            className="cn-fadeUp"
          >
            The Problem
          </div>
          <div
            className="cn-scaleIn"
            style={{
              padding: '36px 40px',
              background: palette.surface,
              border: `1px solid ${palette.border}`,
              borderRadius: 'var(--osd-radius)',
              display: 'flex',
              flexDirection: 'column',
              flex: 1,
              animationDelay: '0.25s',
            }}
          >
          <div style={{ fontSize: 28, lineHeight: 1.55, color: palette.textSoft, flex: 1 }}>
            Academic research output grows{' '}
            <span style={{ color: palette.text, fontWeight: 600 }}>exponentially</span>, producing
            millions of papers annually. Identifying influential publications and uncovering
            thematic communities within massive citation networks is{' '}
            <span style={{ color: palette.amber, fontWeight: 600 }}>intractable</span> with
            traditional tools.
          </div>
          <div
            style={{
              display: 'flex',
              gap: 32,
              marginTop: 32,
              paddingTop: 28,
              borderTop: `1px solid ${palette.border}`,
            }}
          >
            {[
              { num: '~266K', label: 'NYU Papers', color: palette.accent },
              { num: '~25.3M', label: 'Expanded Network', color: palette.accentSoft },
            ].map((s, i) => (
              <div key={i}>
                <div style={{ fontSize: 42, fontWeight: 700, color: s.color, letterSpacing: '-0.02em' }}>
                  {s.num}
                </div>
                <div style={{ fontSize: 20, color: palette.muted, marginTop: 4 }}>{s.label}</div>
              </div>
            ))}
          </div>
          </div>
        </div>

        {/* Objectives */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
          <div
            style={{
              fontFamily: font.mono,
              fontSize: 18,
              letterSpacing: '0.15em',
              textTransform: 'uppercase',
              color: palette.mint,
            }}
            className="cn-fadeUp"
          >
            Objectives
          </div>
          {[
            {
              num: '01',
              title: 'Scalable Graph Pipeline',
              desc: 'Build an end-to-end analytics pipeline using Apache Spark and GraphFrames to process millions of citation relationships.',
              color: palette.mint,
            },
            {
              num: '02',
              title: 'Community Detection',
              desc: 'Apply Louvain algorithm to partition the citation graph into thematic research clusters based on modularity optimization.',
              color: palette.mint,
            },
            {
              num: '03',
              title: 'Influence Ranking',
              desc: 'Implement weighted PageRank to identify and rank the most influential publications within each community.',
              color: palette.mint,
            },
            {
              num: '04',
              title: 'Interactive Visualization',
              desc: 'Generate explorable PyVis network graphs — nodes colored by community, sized by PageRank score.',
              color: palette.mint,
            },
          ].map((obj, i) => (
            <div
              key={i}
              className="cn-slideRight"
              style={{
                display: 'flex',
                gap: 20,
                alignItems: 'flex-start',
                padding: '20px 24px',
                background: palette.surface,
                border: `1px solid ${palette.border}`,
                borderRadius: 'var(--osd-radius)',
                animationDelay: `${0.4 + i * 0.15}s`,
              }}
            >
              <div
                style={{
                  fontFamily: font.mono,
                  fontSize: 20,
                  fontWeight: 700,
                  color: obj.color,
                  flexShrink: 0,
                  marginTop: 2,
                }}
              >
                {obj.num}
              </div>
              <div>
                <div style={{ fontSize: 24, fontWeight: 600, marginBottom: 4 }}>{obj.title}</div>
                <div style={{ fontSize: 21, color: palette.textSoft, lineHeight: 1.45 }}>
                  {obj.desc}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
    <PageNum n={3} />
  </div>
);

// ═══════════════════════════════════════════════════════════════════════════════
// PAGE 4 — Dataset Source
// ═══════════════════════════════════════════════════════════════════════════════
const Dataset: Page = () => (
  <div style={fill}>
    <Styles />
    <GridBg />
    <NyuLogo />
    <div
      style={{
        position: 'absolute',
        inset: 0,
        padding: '90px 140px',
        display: 'flex',
        flexDirection: 'column',
        gap: 36,
      }}
    >
      <div className="cn-fadeUp">
        <Eyebrow>Dataset Source</Eyebrow>
        <h2
          style={{
            marginTop: 20,
            marginBottom: 0,
            fontFamily: 'var(--osd-font-display)',
            fontSize: 80,
            fontWeight: 600,
            letterSpacing: '-0.035em',
            lineHeight: 1.02,
          }}
        >
          OpenAlex — open scholarly{' '}
          <span style={{ color: palette.mint }}>metadata.</span>
        </h2>
      </div>

      {/* Stats row */}
      <div
        className="cn-fadeUp"
        style={{ display: 'flex', gap: 24, animationDelay: '0.25s' }}
      >
        {[
          { num: '~266,200', label: 'Direct NYU Papers', color: palette.accent },
          { num: '~25.3M', label: 'Expanded Citation Network', color: palette.accentSoft },
        ].map((s, i) => (
          <div
            key={i}
            style={{
              flex: 1,
              padding: '24px 28px',
              background: palette.surface,
              border: `1px solid ${palette.border}`,
              borderRadius: 'var(--osd-radius)',
            }}
          >
            <div style={{ fontSize: 36, fontWeight: 700, color: s.color, letterSpacing: '-0.02em' }}>
              {s.num}
            </div>
            <div style={{ fontSize: 19, color: palette.muted, marginTop: 6 }}>{s.label}</div>
          </div>
        ))}
      </div>

      <div style={{ flex: 1, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 32, minHeight: 0 }}>
        {/* Key attributes */}
        <div
          className="cn-scaleIn"
          style={{
            padding: '32px 36px',
            background: palette.surface,
            border: `1px solid ${palette.border}`,
            borderRadius: 'var(--osd-radius)',
            animationDelay: '0.4s',
          }}
        >
          <div
            style={{
              fontFamily: font.mono,
              fontSize: 16,
              letterSpacing: '0.15em',
              textTransform: 'uppercase',
              color: palette.accent,
              marginBottom: 20,
            }}
          >
            Key Attributes Extracted
          </div>
          {[
            'Paper ID — unique OpenAlex work identifier',
            'Title — publication title text',
            'Publication Year — year of publication',
            'Authors — names and institutional affiliations',
            'Referenced Works — outgoing citation links',
            'Cited-by Count — incoming citation count',
            'Primary Location — venue / journal source',
          ].map((attr, i) => (
            <div
              key={i}
              className="cn-slideRight"
              style={{
                padding: '10px 0',
                fontSize: 22,
                color: palette.textSoft,
                borderBottom: i < 6 ? `1px solid ${palette.border}` : 'none',
                animationDelay: `${0.5 + i * 0.08}s`,
              }}
            >
              <span style={{ color: palette.text, fontWeight: 600 }}>
                {attr.split(' — ')[0]}
              </span>
              <span style={{ color: palette.muted }}>{' — '}{attr.split(' — ')[1]}</span>
            </div>
          ))}
        </div>

        {/* API Mechanism */}
        <div
          className="cn-scaleIn"
          style={{
            padding: '32px 36px',
            background: palette.surface,
            border: `1px solid ${palette.border}`,
            borderRadius: 'var(--osd-radius)',
            display: 'flex',
            flexDirection: 'column',
            animationDelay: '0.5s',
          }}
        >
          <div
            style={{
              fontFamily: font.mono,
              fontSize: 16,
              letterSpacing: '0.15em',
              textTransform: 'uppercase',
              color: palette.mint,
              marginBottom: 20,
            }}
          >
            API Mechanism
          </div>
          <div style={{ fontSize: 24, lineHeight: 1.6, color: palette.textSoft, marginBottom: 28 }}>
            REST API with institution filtering, pagination support (max 100 per page),
            and batch lookup capabilities (up to 50 IDs via OR-filter syntax).
          </div>
          <div
            style={{
              fontFamily: font.mono,
              fontSize: 16,
              letterSpacing: '0.15em',
              textTransform: 'uppercase',
              color: palette.amber,
              marginBottom: 16,
            }}
          >
            Search Modules
          </div>
          {[
            { module: 'keyword_search', desc: 'Free-text search with star topology fallback' },
            { module: 'paper_id_search', desc: '1-hop citation expansion from seed papers' },
            { module: 'publication_search', desc: 'Venue + year filter with bidirectional edges' },
          ].map((m, i) => (
            <div
              key={i}
              className="cn-slideRight"
              style={{
                display: 'flex',
                alignItems: 'baseline',
                gap: 14,
                padding: '10px 0',
                borderBottom: i < 2 ? `1px solid ${palette.border}` : 'none',
                animationDelay: `${0.7 + i * 0.12}s`,
              }}
            >
              <span
                style={{
                  fontFamily: font.mono,
                  fontSize: 19,
                  color: palette.amber,
                  flexShrink: 0,
                }}
              >
                {m.module}
              </span>
              <span style={{ fontSize: 20, color: palette.muted }}>{m.desc}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
    <PageNum n={4} />
  </div>
);

// ═══════════════════════════════════════════════════════════════════════════════
// PAGE 5 — Graph Analytics Methods
// ═══════════════════════════════════════════════════════════════════════════════
const Methods: Page = () => (
  <div style={fill}>
    <Styles />
    <GridBg />
    <NyuLogo />
    <div
      style={{
        position: 'absolute',
        inset: 0,
        padding: '90px 140px',
        display: 'flex',
        flexDirection: 'column',
        gap: 36,
      }}
    >
      <div className="cn-fadeUp">
        <Eyebrow>Graph Analytics Methods</Eyebrow>
        <h2
          style={{
            marginTop: 20,
            marginBottom: 0,
            fontFamily: 'var(--osd-font-display)',
            fontSize: 80,
            fontWeight: 600,
            letterSpacing: '-0.035em',
            lineHeight: 1.02,
          }}
        >
          From raw citations to{' '}
          <span
            style={{
              background: `linear-gradient(90deg, ${palette.accentSoft}, var(--osd-accent))`,
              WebkitBackgroundClip: 'text',
              backgroundClip: 'text',
              color: 'transparent',
            }}
          >
            insights.
          </span>
        </h2>
      </div>

      <div style={{ flex: 1, display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 32, minHeight: 0 }}>
        {/* Card 1 — Citation Network Model */}
        <div
          className="cn-scaleIn"
          style={{
            padding: '36px 36px 28px',
            background: palette.surface,
            border: `1px solid ${palette.border}`,
            borderRadius: 'var(--osd-radius)',
            display: 'flex',
            flexDirection: 'column',
            animationDelay: '0.3s',
          }}
        >
          <div
            style={{
              fontFamily: font.mono,
              fontSize: 16,
              letterSpacing: '0.15em',
              textTransform: 'uppercase',
              color: palette.accent,
              marginBottom: 20,
            }}
          >
            Citation Network Model
          </div>
          <div style={{ fontSize: 24, lineHeight: 1.55, color: palette.textSoft, flex: 1 }}>
            <p style={{ margin: '0 0 16px' }}>
              <span style={{ color: palette.accent, fontWeight: 600 }}>Nodes</span> — each dot is
              a research paper, carrying info like its title, authors, and year.
            </p>
            <p style={{ margin: 0 }}>
              <span style={{ color: palette.accentSoft, fontWeight: 600 }}>Edges</span> — an arrow
              from Paper A to Paper B means "A cited B." Follow the arrows to see how ideas spread.
            </p>
          </div>
          {/* Animated directed graph */}
          <svg width="100%" height="160" viewBox="0 0 460 160" style={{ marginTop: 12 }}>
            <defs>
              <marker id="cn-arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
                <path d="M 0 0 L 10 5 L 0 10 z" fill={palette.accent} />
              </marker>
              <marker id="cn-arr2" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
                <path d="M 0 0 L 10 5 L 0 10 z" fill={palette.accentSoft} />
              </marker>
            </defs>
            {/* Edges — drawn with animation */}
            {[
              [60, 80, 140, 35, palette.accent],
              [60, 80, 140, 80, palette.accent],
              [60, 80, 130, 130, palette.accentSoft],
              [140, 35, 240, 25, palette.accent],
              [140, 35, 240, 70, palette.accentSoft],
              [140, 80, 240, 70, palette.accent],
              [140, 80, 240, 120, palette.accent],
              [130, 130, 240, 120, palette.accentSoft],
              [240, 25, 340, 45, palette.accent],
              [240, 70, 340, 45, palette.accentSoft],
              [240, 70, 340, 100, palette.accent],
              [240, 120, 340, 100, palette.accent],
              [240, 120, 340, 145, palette.accentSoft],
              [340, 45, 420, 60, palette.accent],
              [340, 100, 420, 60, palette.accentSoft],
              [340, 100, 420, 130, palette.accent],
              [340, 145, 420, 130, palette.accent],
            ].map(([x1, y1, x2, y2, color], i) => (
              <line
                key={i}
                x1={x1 as number} y1={y1 as number} x2={x2 as number} y2={y2 as number}
                stroke={color as string}
                strokeWidth={1}
                strokeDasharray="200"
                strokeDashoffset="200"
                markerEnd={color === palette.accent ? 'url(#cn-arr)' : 'url(#cn-arr2)'}
                style={{ animation: `cn-drawLine 1s ease forwards ${0.8 + i * 0.08}s` }}
              />
            ))}
            {/* Nodes — appear with pulse */}
            {[
              { cx: 60, cy: 80, r: 10, color: palette.accent, delay: 0.5 },
              { cx: 140, cy: 35, r: 7, color: palette.accentSoft, delay: 0.7 },
              { cx: 140, cy: 80, r: 8, color: palette.accent, delay: 0.8 },
              { cx: 130, cy: 130, r: 6, color: palette.accentSoft, delay: 0.9 },
              { cx: 240, cy: 25, r: 5, color: palette.mint, delay: 1.0 },
              { cx: 240, cy: 70, r: 9, color: palette.accent, delay: 1.1 },
              { cx: 240, cy: 120, r: 7, color: palette.mint, delay: 1.2 },
              { cx: 340, cy: 45, r: 8, color: palette.accentSoft, delay: 1.3 },
              { cx: 340, cy: 100, r: 11, color: palette.accent, delay: 1.4 },
              { cx: 340, cy: 145, r: 5, color: palette.mint, delay: 1.5 },
              { cx: 420, cy: 60, r: 6, color: palette.accentSoft, delay: 1.6 },
              { cx: 420, cy: 130, r: 7, color: palette.accent, delay: 1.7 },
            ].map((n, i) => (
              <g key={i}>
                <circle
                  cx={n.cx} cy={n.cy} r={n.r + 6}
                  fill="none"
                  stroke={n.color}
                  strokeWidth={0.5}
                  opacity={0}
                  style={{ animation: `cn-pulse 3s ease-in-out infinite ${n.delay + 1}s, cn-fadeIn 0.5s ease forwards ${n.delay}s` }}
                />
                <circle
                  cx={n.cx} cy={n.cy} r={n.r}
                  fill={n.color}
                  opacity={0}
                  style={{ animation: `cn-scaleIn 0.4s cubic-bezier(.2,.7,.2,1) forwards ${n.delay}s` }}
                />
              </g>
            ))}
          </svg>
        </div>

        {/* Card 2 — Louvain Community Detection */}
        <div
          className="cn-scaleIn"
          style={{
            padding: '36px 36px 28px',
            background: palette.surface,
            border: `1px solid ${palette.border}`,
            borderRadius: 'var(--osd-radius)',
            display: 'flex',
            flexDirection: 'column',
            animationDelay: '0.45s',
          }}
        >
          <div
            style={{
              fontFamily: font.mono,
              fontSize: 16,
              letterSpacing: '0.15em',
              textTransform: 'uppercase',
              color: palette.mint,
              marginBottom: 20,
            }}
          >
            Louvain Community Detection
          </div>
          <div style={{ fontSize: 24, lineHeight: 1.55, color: palette.textSoft, flex: 1 }}>
            <p style={{ margin: '0 0 16px' }}>
              Imagine papers that cite each other a lot are <span style={{ color: palette.mint, fontWeight: 600 }}>friends</span>.
              Louvain groups friends together into <span style={{ color: palette.mint, fontWeight: 600 }}>communities</span> —
              papers in the same group talk to each other more than to outsiders.
            </p>
            <p style={{ margin: 0 }}>
              Each community gets its own color in the visualization, so you can instantly spot
              research clusters at a glance.
            </p>
          </div>
        </div>

        {/* Card 3 — Weighted PageRank */}
        <div
          className="cn-scaleIn"
          style={{
            padding: '36px 36px 28px',
            background: palette.surface,
            border: `1px solid ${palette.border}`,
            borderRadius: 'var(--osd-radius)',
            display: 'flex',
            flexDirection: 'column',
            animationDelay: '0.6s',
          }}
        >
          <div
            style={{
              fontFamily: font.mono,
              fontSize: 16,
              letterSpacing: '0.15em',
              textTransform: 'uppercase',
              color: palette.amber,
              marginBottom: 20,
            }}
          >
            Weighted PageRank
          </div>
          <div style={{ fontSize: 24, lineHeight: 1.55, color: palette.textSoft, flex: 1 }}>
            <p style={{ margin: '0 0 16px' }}>
              A paper cited by many <span style={{ color: palette.amber, fontWeight: 600 }}>highly-cited papers</span> is
              more important than one cited by obscure ones. PageRank captures this recursive influence.
            </p>
            <p style={{ margin: 0 }}>
              Bigger nodes in the visualization = higher PageRank = more influential.
            </p>
          </div>
        </div>
      </div>
    </div>
    <PageNum n={5} />
  </div>
);

// ═══════════════════════════════════════════════════════════════════════════════
// PAGE 6 — Technologies Used
// ═══════════════════════════════════════════════════════════════════════════════
const TechStack: Page = () => {
  const stack = [
    { layer: 'Ingestion', component: 'API Client', tech: 'Python requests + OpenAlex REST API', color: palette.accent },
    { layer: 'Storage', component: 'Distributed Store', tech: 'Parquet via PyArrow / HDFS', color: palette.accentSoft },
    { layer: 'Processing', component: 'Spark Session', tech: 'Apache PySpark (DataFrames)', color: palette.mint },
    { layer: 'Graph', component: 'Construction', tech: 'GraphFrames', color: palette.accent2 },
    { layer: 'Analytics', component: 'Community / Centrality', tech: 'Louvain + Weighted PageRank', color: palette.amber },
    { layer: 'Orchestration', component: 'Notebook', tech: 'Jupyter', color: palette.mint },
    { layer: 'Visualization', component: 'Graph Renderer', tech: 'PyVis 0.3.1', color: palette.accentSoft },
  ];

  return (
    <div style={fill}>
      <Styles />
      <GridBg />
      <NyuLogo />
      <div
        style={{
          position: 'absolute',
          inset: 0,
          padding: '70px 140px',
          display: 'flex',
          flexDirection: 'column',
          gap: 24,
        }}
      >
        <div className="cn-fadeUp">
          <Eyebrow>Technologies Used</Eyebrow>
          <h2
            style={{
              marginTop: 16,
              marginBottom: 0,
              fontFamily: 'var(--osd-font-display)',
              fontSize: 72,
              fontWeight: 600,
              letterSpacing: '-0.035em',
              lineHeight: 1.02,
            }}
          >
            The tech stack.
          </h2>
        </div>

        {/* Table header */}
        <div
          className="cn-fadeUp"
          style={{
            display: 'grid',
            gridTemplateColumns: '200px 240px 1fr',
            animationDelay: '0.25s',
          }}
        >
          {['Layer', 'Component', 'Technology'].map((h) => (
            <div
              key={h}
              style={{
                padding: '10px 24px',
                fontFamily: font.mono,
                fontSize: 17,
                letterSpacing: '0.15em',
                textTransform: 'uppercase',
                color: palette.muted,
                borderBottom: `2px solid ${palette.borderBright}`,
              }}
            >
              {h}
            </div>
          ))}
        </div>

        {/* Table rows */}
        {stack.map((row, i) => (
          <div
            key={i}
            className="cn-slideRight"
            style={{
              display: 'grid',
              gridTemplateColumns: '200px 240px 1fr',
              borderBottom: `1px solid ${palette.border}`,
              animationDelay: `${0.35 + i * 0.09}s`,
            }}
          >
            <div style={{ padding: '12px 24px', fontSize: 24, fontWeight: 600, color: row.color }}>
              {row.layer}
            </div>
            <div style={{ padding: '12px 24px', fontSize: 24, color: palette.textSoft }}>
              {row.component}
            </div>
            <div style={{ padding: '12px 24px', fontSize: 22, display: 'flex', alignItems: 'center' }}>
              <span
                style={{
                  fontFamily: font.mono,
                  fontSize: 20,
                  background: `${row.color}12`,
                  border: `1px solid ${row.color}35`,
                  padding: '4px 14px',
                  borderRadius: 8,
                }}
              >
                {row.tech}
              </span>
            </div>
          </div>
        ))}
      </div>
      <PageNum n={6} />
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════════════════════
// PAGE 7 — Data Flow
// ═══════════════════════════════════════════════════════════════════════════════
const DataFlow: Page = () => {
  const steps = [
    { num: '01', label: 'User Query', desc: 'Keyword / Paper ID / Pub + Year via Jupyter widget', color: palette.accent },
    { num: '02', label: 'OpenAlex API', desc: 'REST call with NYU institution filter', color: palette.accent },
    { num: '03', label: 'Parse & Dedup', desc: 'JSON parsed, normalized, deduplicated', color: palette.accentSoft },
    { num: '04', label: 'Spark Ingest', desc: 'Data loaded into PySpark DataFrames', color: palette.accentSoft },
    { num: '05', label: 'Graph Schema', desc: 'Vertices (papers) + Edges (citations)', color: palette.mint },
    { num: '10', label: 'Explore', desc: 'User explores graph in browser', color: palette.accent2 },
    { num: '09', label: 'PyVis Render', desc: 'Interactive HTML with colored nodes', color: palette.accent2 },
    { num: '08', label: 'PageRank', desc: 'Weighted ranking of influential papers', color: palette.amber },
    { num: '07', label: 'Louvain', desc: 'Community detection → research clusters', color: palette.amber },
    { num: '06', label: 'GraphFrames', desc: 'Builds directed citation graph', color: palette.mint },
  ];

  return (
    <div style={fill}>
      <Styles />
      <GridBg />
      <NyuLogo />
      <div
        style={{
          position: 'absolute',
          inset: 0,
          padding: '80px 120px',
          display: 'flex',
          flexDirection: 'column',
          gap: 32,
        }}
      >
        <div className="cn-fadeUp">
          <Eyebrow>Data Flow &amp; Architecture</Eyebrow>
          <h2
            style={{
              marginTop: 20,
              marginBottom: 0,
              fontFamily: 'var(--osd-font-display)',
              fontSize: 76,
              fontWeight: 600,
              letterSpacing: '-0.035em',
              lineHeight: 1.02,
            }}
          >
            End-to-end pipeline.
          </h2>
        </div>

        <div
          style={{
            flex: 1,
            display: 'grid',
            gridTemplateColumns: 'repeat(5, 1fr)',
            gridTemplateRows: 'repeat(2, 1fr)',
            gap: 18,
            minHeight: 0,
          }}
        >
          {steps.map((step, i) => (
            <div
              key={i}
              className="cn-scaleIn"
              style={{
                position: 'relative',
                padding: '22px 20px',
                background: palette.surface,
                border: `1px solid ${palette.border}`,
                borderRadius: 'var(--osd-radius)',
                display: 'flex',
                flexDirection: 'column',
                animationDelay: `${0.3 + i * 0.1}s`,
              }}
            >
              <div
                style={{
                  fontFamily: font.mono,
                  fontSize: 18,
                  fontWeight: 700,
                  color: step.color,
                  marginBottom: 10,
                }}
              >
                {step.num}
              </div>
              <div style={{ fontSize: 22, fontWeight: 600, marginBottom: 6 }}>{step.label}</div>
              <div style={{ fontSize: 18, color: palette.muted, lineHeight: 1.4 }}>{step.desc}</div>
              {/* Row 1: arrow right (not last) */}
              {i < 4 && (
                <div
                  style={{
                    position: 'absolute',
                    right: -13,
                    top: '50%',
                    transform: 'translateY(-50%)',
                    color: palette.dim,
                    fontSize: 18,
                    zIndex: 2,
                  }}
                >
                  →
                </div>
              )}
              {/* Down arrow from end of row 1 to start of row 2 */}
              {i === 4 && (
                <div
                  style={{
                    position: 'absolute',
                    bottom: -14,
                    right: 24,
                    color: palette.dim,
                    fontSize: 18,
                    zIndex: 2,
                  }}
                >
                  ↓
                </div>
              )}
              {/* Row 2: arrow left between cards */}
              {i >= 5 && i < 9 && (
                <div
                  style={{
                    position: 'absolute',
                    right: -13,
                    top: '50%',
                    transform: 'translateY(-50%)',
                    color: palette.dim,
                    fontSize: 18,
                    zIndex: 2,
                  }}
                >
                  ←
                </div>
              )}
            </div>
          ))}
        </div>

        {/* Legend */}
        <div
          className="cn-fadeUp"
          style={{
            display: 'flex',
            gap: 32,
            justifyContent: 'center',
            animationDelay: '1.5s',
          }}
        >
          {[
            { label: 'Ingestion', color: palette.accent },
            { label: 'Processing', color: palette.accentSoft },
            { label: 'Graph Build', color: palette.mint },
            { label: 'Analytics', color: palette.amber },
            { label: 'Output', color: palette.accent2 },
          ].map((l, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <div style={{ width: 10, height: 10, borderRadius: 3, background: l.color }} />
              <span style={{ fontSize: 18, color: palette.muted }}>{l.label}</span>
            </div>
          ))}
        </div>
      </div>
      <PageNum n={7} />
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════════════════════
// PAGE 8 — Closing / Thank You
// ═══════════════════════════════════════════════════════════════════════════════
const Closing: Page = () => (
  <div style={fill}>
    <Styles />
    <GridBg />
    <NetworkBgSvg opacity={0.05} />
    <NyuLogo />
    <div
      style={{
        position: 'absolute',
        inset: 0,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        textAlign: 'center',
      }}
    >
      <h1
        className="cn-fadeUp"
        style={{
          fontFamily: 'var(--osd-font-display)',
          fontSize: 'var(--osd-size-hero)',
          lineHeight: 0.98,
          fontWeight: 600,
          margin: 0,
          letterSpacing: '-0.045em',
          animationDelay: '0.15s',
        }}
      >
        Thank{' '}
        <span
          style={{
            background: `linear-gradient(90deg, ${palette.accentSoft}, var(--osd-accent))`,
            WebkitBackgroundClip: 'text',
            backgroundClip: 'text',
            color: 'transparent',
          }}
        >
          You!
        </span>
      </h1>
      <p
        className="cn-fadeUp"
        style={{
          marginTop: 36,
          maxWidth: 800,
          fontSize: 'var(--osd-size-body)',
          lineHeight: 1.35,
          color: palette.textSoft,
          animationDelay: '0.35s',
        }}
      >
        Graph-Based Analysis of Academic Citation Networks
      </p>

      <div
        className="cn-fadeUp"
        style={{
          display: 'flex',
          gap: 36,
          marginTop: 56,
          animationDelay: '0.55s',
        }}
      >
        {[
          { name: 'Navya Gupta', id: 'NG3118' },
          { name: 'Hardik Amarwani', id: 'HA3290' },
          { name: 'Shreyansh Saurabh', id: 'SS21034' },
        ].map((m, i) => (
          <div
            key={i}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 14,
              padding: '12px 22px',
              background: palette.surface,
              border: `1px solid ${palette.border}`,
              borderRadius: 'var(--osd-radius)',
            }}
          >
            <div
              style={{
                width: 38,
                height: 38,
                borderRadius: '50%',
                background: `linear-gradient(135deg, ${palette.accent}, ${palette.accentSoft})`,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontSize: 17,
                fontWeight: 700,
              }}
            >
              {m.name[0]}
            </div>
            <div>
              <div style={{ fontSize: 22, fontWeight: 600 }}>{m.name}</div>
              <div style={{ fontSize: 16, fontFamily: font.mono, color: palette.muted }}>
                {m.id}
              </div>
            </div>
          </div>
        ))}
      </div>

      <div
        className="cn-fadeUp"
        style={{
          marginTop: 32,
          fontFamily: font.mono,
          fontSize: 22,
          color: palette.muted,
          animationDelay: '0.7s',
        }}
      >
        Big Data · CS-GY 6513 C · Spring 2026
      </div>
    </div>
    <PageNum n={8} />
  </div>
);

// ─── Slide export ─────────────────────────────────────────────────────────────
export const meta: SlideMeta = {
  title: 'Graph-Based Analysis of Academic Citation Networks',
};

export default [
  Cover,
  Abstract,
  ProblemObjectives,
  Dataset,
  Methods,
  TechStack,
  DataFlow,
  Closing,
] satisfies Page[];
