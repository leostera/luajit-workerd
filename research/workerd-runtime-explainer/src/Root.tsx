import React from 'react';
import {Composition} from 'remotion';
import {Explainer, FPS, SCENE_FRAMES, SCENE_COUNT} from './Explainer';

export const Root: React.FC = () => (
  <Composition
    id="WorkerdRuntimeExplainer"
    component={Explainer}
    durationInFrames={SCENE_FRAMES * SCENE_COUNT}
    fps={FPS}
    width={1920}
    height={1080}
  />
);
