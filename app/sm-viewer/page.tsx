"use client";

import type { SingleMoleculeDataset } from "@/lib/SingleMoleculeDataset";
import type { PanelType } from "@/lib/stores/splitScreenStore";

import { Suspense, useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { Button, Spinner } from "@heroui/react";

import { SingleMoleculeThreeScene } from "@/components/single-molecule-three-scene";
import { SingleMoleculeControls } from "@/components/single-molecule-controls";
import { SingleMoleculeLegends } from "@/components/single-molecule-legends";
import { SplitScreenContainer } from "@/components/split-screen-container";
import { useSingleMoleculeStore } from "@/lib/stores/singleMoleculeStore";
import { pickDefaultGenes } from "@/lib/utils/auto-select-genes";
import { useSingleMoleculeVisualizationStore } from "@/lib/stores/singleMoleculeVisualizationStore";
import { useSplitScreenStore } from "@/lib/stores/splitScreenStore";
import {
  useSMVizUrlSync,
  tryReadSMVizFromUrl,
  applySMVizState,
} from "@/lib/hooks/useUrlVizSync";
import LightRays from "@/components/react-bits/LightRays";
import { subtitle, title } from "@/components/primitives";

function SingleMoleculeViewerContent() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const { addDataset } = useSingleMoleculeStore();
  const smVizStore = useSingleMoleculeVisualizationStore();
  const { addGene } = smVizStore;
  const {
    isSplitMode,
    rightPanelDatasetId,
    rightPanelS3Url,
    rightPanelType,
    syncEnabled,
    enableSplit,
    setRightPanel,
    setRightPanelS3,
    setSyncEnabled,
    setSyncFromUrl,
  } = useSplitScreenStore();
  const [dataset, setDataset] = useState<SingleMoleculeDataset | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const s3Url = searchParams.get("url");
  const owner = searchParams.get("owner");
  const datasetName = searchParams.get("dataset");
  const versionName = searchParams.get("version");
  const datasetRoot = searchParams.get("datasetRoot") || "";
  const admapPath =
    owner && datasetName && versionName
      ? "/" +
        [owner, datasetName, versionName, datasetRoot]
          .filter(Boolean)
          .join("/")
      : null;
  const sourceUrl = s3Url || admapPath;

  useSMVizUrlSync(!!dataset, dataset, smVizStore);

  // Read split params from URL on mount
  useEffect(() => {
    const splitId = searchParams.get("split");
    const splitS3Url = searchParams.get("splitS3Url");
    const splitType = searchParams.get("splitType") as PanelType | null;

    if (splitS3Url && splitType) {
      enableSplit();
      setRightPanelS3(decodeURIComponent(splitS3Url), splitType);
    } else if (splitId && splitType) {
      enableSplit();
      setRightPanel(splitId, splitType);
    }

    if (searchParams.get("sync") === "1") {
      setSyncEnabled(true);
      setSyncFromUrl(true);
    }
  }, []);

  // Write split params to URL when split state changes
  useEffect(() => {
    const newParams = new URLSearchParams(window.location.search);

    if (isSplitMode && rightPanelType) {
      if (rightPanelS3Url) {
        newParams.set("splitS3Url", encodeURIComponent(rightPanelS3Url));
        newParams.delete("split");
      } else if (rightPanelDatasetId) {
        newParams.set("split", rightPanelDatasetId);
        newParams.delete("splitS3Url");
      }
      newParams.set("splitType", rightPanelType);
      if (syncEnabled) {
        newParams.set("sync", "1");
      } else {
        newParams.delete("sync");
      }
      router.replace(`?${newParams.toString()}`, { scroll: false });
    } else if (!isSplitMode) {
      newParams.delete("split");
      newParams.delete("splitS3Url");
      newParams.delete("splitType");
      newParams.delete("sync");
      const paramStr = newParams.toString();

      router.replace(paramStr ? `?${paramStr}` : ".", {
        scroll: false,
      });
    }
  }, [
    isSplitMode,
    rightPanelDatasetId,
    rightPanelS3Url,
    rightPanelType,
    syncEnabled,
  ]);

  useEffect(() => {
    if (!sourceUrl) {
      setError(
        "No dataset specified. Open a dataset from the ADMAP Dataset Detail page, or provide ?url=... directly.",
      );
      setIsLoading(false);

      return;
    }

    loadDataset(sourceUrl);
  }, [sourceUrl]);

  const loadDataset = async (baseUrl: string) => {
    try {
      setIsLoading(true);
      setError(null);

      console.log("Loading single molecule dataset:", baseUrl);

      const { SingleMoleculeDataset } = await import(
        "@/lib/SingleMoleculeDataset"
      );

      const smDataset = await SingleMoleculeDataset.fromCustomS3(
        baseUrl,
        (progress, message) => {
          console.log(`${progress}%: ${message}`);
        },
      );

      console.log(
        "SingleMoleculeDataset loaded:",
        smDataset.getSummary(),
      );

      setDataset(smDataset);
      addDataset(smDataset);

      const urlVizState = tryReadSMVizFromUrl("left");

      if (urlVizState) {
        applySMVizState(urlVizState, smVizStore, smDataset);
      } else {
        const genesToSelect = pickDefaultGenes(smDataset.uniqueGenes);

        genesToSelect.forEach((gene) => {
          addGene(gene);
        });
      }

      setIsLoading(false);
    } catch (err) {
      console.error("Error loading single molecule dataset:", err);
      setError(
        err instanceof Error ? err.message : "Failed to load dataset",
      );
      setIsLoading(false);
    }
  };

  const goBack = () => {
    if (window.history.length > 1) {
      router.back();
    } else {
      router.push("/");
    }
  };

  if (isLoading) {
    return (
      <>
        <div className="fixed inset-0 w-full h-full z-0">
          <LightRays
            lightSpread={1.0}
            mouseInfluence={0.1}
            pulsating={false}
            rayLength={10}
            raysColor="#667eea"
            raysOrigin="top-left"
            raysSpeed={0.8}
          />
        </div>
        <div className="fixed inset-0 w-full h-full z-0">
          <LightRays
            lightSpread={1.0}
            mouseInfluence={0.1}
            pulsating={false}
            rayLength={10}
            raysColor="#764ba2"
            raysOrigin="top-right"
            raysSpeed={0.8}
          />
        </div>
        <div className="relative z-10 flex items-center justify-center h-full">
          <div className="flex flex-col items-center gap-4">
            <Spinner color="secondary" size="lg" />
            <p className={subtitle()}>Loading dataset...</p>
          </div>
        </div>
      </>
    );
  }

  if (error) {
    return (
      <>
        <div className="fixed inset-0 w-full h-full z-0">
          <LightRays
            lightSpread={1.0}
            mouseInfluence={0.1}
            pulsating={false}
            rayLength={10}
            raysColor="#FF72E1"
            raysOrigin="top-center"
            raysSpeed={0.8}
          />
        </div>
        <div className="relative z-10 flex items-center justify-center h-full p-8">
          <div className="flex flex-col items-center gap-6 max-w-2xl w-full">
            <div className="text-center">
              <h2 className={title({ size: "md", color: "pink" })}>
                Failed to load dataset
              </h2>
              <p className={subtitle({ class: "mt-4" })}>{error}</p>
            </div>
            <div className="flex gap-4">
              <Button color="secondary" onPress={goBack}>
                Back
              </Button>
              {sourceUrl && (
                <Button
                  color="default"
                  variant="bordered"
                  onPress={() => loadDataset(sourceUrl)}
                >
                  Retry
                </Button>
              )}
            </div>
          </div>
        </div>
      </>
    );
  }

  if (!dataset) {
    return null;
  }

  return (
    <SplitScreenContainer>
      <SingleMoleculeControls />
      <SingleMoleculeLegends />
      <SingleMoleculeThreeScene />
    </SplitScreenContainer>
  );
}

export default function SingleMoleculeViewerPage() {
  return (
    <Suspense
      fallback={
        <div className="flex items-center justify-center h-full">
          <Spinner size="lg" />
        </div>
      }
    >
      <SingleMoleculeViewerContent />
    </Suspense>
  );
}
