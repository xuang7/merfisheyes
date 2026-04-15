"use client";

import { Navbar } from "@/components/navbar";
import { useVisualizationStore } from "@/lib/stores/visualizationStore";

export function NavbarWrapper() {
  const isPlayback = useVisualizationStore((s) => s.celltypePlayback);

  if (isPlayback) return null;

  return <Navbar />;
}
