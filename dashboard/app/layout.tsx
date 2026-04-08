import type { Metadata } from "next";
import { Kanit } from "next/font/google";
import "./globals.css";
import Navbar from "@/components/Navbar";

const kanit = Kanit({
  subsets: ["latin"],
  weight: ["300", "400", "500", "600", "700"],
  variable: "--font-kanit",
  display: "swap",
});

export const metadata: Metadata = {
  title: "SNCF Data Observatory",
  description: "Monitoring temps réel de la qualité des données ferroviaires SNCF",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="fr" className={kanit.variable}>
      <body className="bg-[#07070f] text-white min-h-screen antialiased">
        <Navbar />
        <main className="pt-16">{children}</main>
        <footer className="border-t border-white/5 mt-24 py-8 px-6">
          <div className="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-2">
            <span className="text-xs text-white/20 tracking-widest font-light uppercase">
              SNCF Data Observatory — Michel DUPONT
            </span>
            <span className="text-xs text-white/20 tracking-widest font-light">
              github.com/heykelh/sncf-data-observatory
            </span>
          </div>
        </footer>
      </body>
    </html>
  );
}
