"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";

const links = [
  { href: "/",            label: "Live Monitor" },
  { href: "/history",     label: "Historique"   },
  { href: "/governance",  label: "Gouvernance"  },
];

export default function Navbar() {
  const pathname  = usePathname();
  const [open, setOpen]       = useState(false);
  const [scrolled, setScrolled] = useState(false);

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 20);
    window.addEventListener("scroll", onScroll);
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  // Ferme le menu mobile sur changement de route
  useEffect(() => { setOpen(false); }, [pathname]);

  return (
    <header
      className={`fixed top-0 left-0 right-0 z-50 transition-all duration-300 ${
        scrolled
          ? "bg-[#07070f]/90 backdrop-blur-xl border-b border-white/[0.06]"
          : "bg-transparent"
      }`}
    >
      <div className="max-w-7xl mx-auto px-4 sm:px-6 h-16 flex items-center justify-between">

        {/* Logo */}
        <Link href="/" className="flex items-center gap-3 group">
          <div className="relative flex items-center justify-center w-7 h-7">
            <div className="absolute w-full h-full rounded-sm border border-violet-500/40 rotate-45 group-hover:border-violet-400/70 transition-colors" />
            <div className="w-2 h-2 rounded-sm bg-violet-500 rotate-45 group-hover:bg-violet-400 transition-colors"
                 style={{ boxShadow: "0 0 8px rgba(139,92,246,0.8)" }} />
          </div>
          <div className="flex flex-col leading-none">
            <span className="font-kanit font-600 text-sm tracking-[0.12em] text-white uppercase">
              SNCF Observatory
            </span>
            <span className="font-kanit font-300 text-[9px] tracking-[0.25em] text-violet-400/60 uppercase">
              Data Quality Monitor
            </span>
          </div>
        </Link>

        {/* Desktop nav */}
        <nav className="hidden md:flex items-center gap-1">
          {links.map((link) => {
            const active = pathname === link.href;
            return (
              <Link
                key={link.href}
                href={link.href}
                className={`relative px-4 py-2 text-sm font-400 tracking-wide transition-colors duration-200 rounded-md ${
                  active
                    ? "text-white"
                    : "text-white/40 hover:text-white/80"
                }`}
              >
                {active && (
                  <span
                    className="absolute inset-0 rounded-md bg-violet-500/10 border border-violet-500/20"
                    style={{ boxShadow: "inset 0 0 12px rgba(139,92,246,0.05)" }}
                  />
                )}
                <span className="relative">{link.label}</span>
                {active && (
                  <span className="absolute bottom-0 left-1/2 -translate-x-1/2 w-1 h-1 rounded-full bg-violet-400"
                        style={{ boxShadow: "0 0 6px rgba(139,92,246,0.9)" }} />
                )}
              </Link>
            );
          })}
        </nav>

        {/* Live indicator — desktop */}
        <div className="hidden md:flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-blink"
                style={{ boxShadow: "0 0 6px rgba(16,185,129,0.8)" }} />
          <span className="text-[10px] font-500 tracking-[0.2em] text-emerald-400/70 uppercase">Live</span>
        </div>

        {/* Burger — mobile */}
        <button
          onClick={() => setOpen(!open)}
          className="md:hidden flex flex-col gap-1.5 p-2 rounded-md hover:bg-white/5 transition-colors"
          aria-label="Menu"
        >
          <span className={`block w-5 h-px bg-white/60 transition-all duration-300 ${open ? "rotate-45 translate-y-[7px]" : ""}`} />
          <span className={`block w-5 h-px bg-white/60 transition-all duration-300 ${open ? "opacity-0" : ""}`} />
          <span className={`block w-5 h-px bg-white/60 transition-all duration-300 ${open ? "-rotate-45 -translate-y-[7px]" : ""}`} />
        </button>
      </div>

      {/* Mobile menu */}
      <div className={`md:hidden transition-all duration-300 overflow-hidden ${open ? "max-h-64 opacity-100" : "max-h-0 opacity-0"}`}>
        <div className="border-t border-white/[0.06] bg-[#07070f]/95 backdrop-blur-xl px-4 py-4 flex flex-col gap-1">
          {links.map((link) => {
            const active = pathname === link.href;
            return (
              <Link
                key={link.href}
                href={link.href}
                className={`px-4 py-3 rounded-md text-sm font-400 tracking-wide transition-colors ${
                  active
                    ? "bg-violet-500/10 border border-violet-500/20 text-white"
                    : "text-white/40 hover:text-white/70 hover:bg-white/5"
                }`}
              >
                {link.label}
              </Link>
            );
          })}
          <div className="flex items-center gap-2 px-4 pt-2 mt-1 border-t border-white/[0.04]">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-blink"
                  style={{ boxShadow: "0 0 6px rgba(16,185,129,0.8)" }} />
            <span className="text-[10px] font-500 tracking-[0.2em] text-emerald-400/70 uppercase">Live</span>
          </div>
        </div>
      </div>
    </header>
  );
}
